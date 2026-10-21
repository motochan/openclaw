import { afterEach, describe, expect, it } from "vitest";
import { chunkText } from "../../../auto-reply/chunk.js";
import {
  initializeGlobalHookRunner,
  resetGlobalHookRunner,
} from "../../../plugins/hook-runner-global.js";
import { addTestHook } from "../../../plugins/hooks.test-helpers.js";
import { setActivePluginRegistry } from "../../../plugins/runtime.js";
import { matchesTranscriptEvent } from "../../../sessions/transcript-visible-record.js";
import {
  createOutboundTestPlugin,
  createTestRegistry,
} from "../../../test-utils/channel-plugins.js";
import { buildAgentRunTerminalReplySnapshot } from "../../agent-run-terminal-reply.js";
import { taskCompletionEvents } from "../../subagent-test-fixtures.test-helpers.js";
import type { SubagentRunRecord } from "../registry/subagent-registry.types.js";
import { deliverCompletionDirect } from "./subagent-announce-completion-delivery.js";
import { readSubagentRunAnnounceResultUsing } from "./subagent-announce-result.js";

const text = "x".repeat(5_000) + "é尾";
afterEach(() => {
  resetGlobalHookRunner();
  setActivePluginRegistry(createTestRegistry());
});

async function fixture(
  options: {
    retained?: boolean;
    hook?: { content?: string; cancel?: boolean };
    onHook?: (child: SubagentRunRecord) => void;
  } = {},
) {
  const child: SubagentRunRecord = {
    runId: "child-run",
    childSessionKey: "agent:main:subagent:child",
    requesterSessionKey: "agent:main:telegram:direct:12345",
    requesterDisplayKey: "main",
    task: "Complete result",
    cleanup: "keep",
    createdAt: 1,
    generation: 2,
    execution: { status: "terminal", outcome: { status: "ok" } },
    completion: {
      required: true,
      terminalReply: buildAgentRunTerminalReplySnapshot({ visibleText: text }),
    },
  };
  const event = {
    type: "message",
    message: {
      role: "assistant",
      stopReason: "stop",
      content: [{ type: "text", text }],
      __openclaw: { runId: child.runId },
    },
  };
  const prepared = await readSubagentRunAnnounceResultUsing(child, {
    readSubagentRun: () => child,
    getRuntimeConfig: () => ({}),
    resolveAgentIdFromSessionKey: () => "main",
    resolveSessionStorePathCore: () => "/tmp/synthetic-store",
    readSubagentSessionEntry: () => ({ sessionId: "child-session", updatedAt: 1 }),
    findTranscriptEvent: async (_scope, match) =>
      options.retained !== false && matchesTranscriptEvent(event, match) ? { event } : undefined,
    findSessionTranscriptArchiveEventReadOnly: async () => undefined,
  });
  const received: string[] = [];
  const plugin = createOutboundTestPlugin({
    id: "telegram",
    outbound: {
      deliveryMode: "direct",
      chunker: chunkText,
      textChunkLimit: 2_000,
      sendText: async ({ text: chunk }) => {
        received.push(chunk);
        return { channel: "telegram", messageId: String(received.length) };
      },
    },
  });
  const registry = createTestRegistry([{ pluginId: "telegram", plugin, source: "test" }]);
  if (options.hook || options.onHook) {
    addTestHook({
      registry,
      pluginId: "telegram",
      hookName: "message_sending",
      conversationAccessAllowed: true,
      handler: () => {
        options.onHook?.(child);
        return options.hook;
      },
    });
  }
  setActivePluginRegistry(registry);
  initializeGlobalHookRunner(registry);
  const params: Parameters<typeof deliverCompletionDirect>[0] = {
    cfg: {},
    requesterSessionKey: child.requesterSessionKey,
    directIdempotencyKey: "presentation-fixture",
    deliveryTarget: { deliver: true, channel: "telegram", to: "12345" },
    internalEvents: taskCompletionEvents({
      childSessionKey: child.childSessionKey,
      result: prepared.text ?? "NO_REPLY",
    }),
    contentKind: "completed_result",
    sourceRunId: child.runId,
    sourceSessionKey: child.childSessionKey,
    preparedResult: prepared,
  };
  return { child, prepared, received, params, deliver: () => deliverCompletionDirect(params) };
}

describe("complete direct result presentation", () => {
  it("materializes the retained full result through actual outbound chunking", async () => {
    const f = await fixture();
    const result = await f.deliver();
    expect(f.received).toHaveLength(3);
    expect(f.received.join("")).toBe(text);
    expect(result?.completeResultPresentation).toEqual(f.prepared.completeResult);
    expect(f.child.completion?.terminalReply).toMatchObject({ text: "x".repeat(4_095) + "…" });
    expect(result).not.toHaveProperty("platformMessageIds");
  });
  it("keeps ordinary unbound parent input bounded and membership UNKNOWN", async () => {
    const f = await fixture();
    delete f.params.preparedResult;
    const result = await f.deliver();
    expect(f.received.join("")).toBe("x".repeat(4_095) + "…");
    expect(result?.completeResultPresentation).toBeUndefined();
  });
  it.each(["run", "child", "hash", "length"] as const)(
    "rejects %s mismatches without claiming complete membership",
    async (mismatch) => {
      const f = await fixture();
      if (mismatch === "run") f.params.sourceRunId = "different-run";
      else if (mismatch === "child")
        f.params.internalEvents![0].childSessionKey = "agent:main:subagent:other";
      else
        f.prepared.completeResult = {
          ...f.prepared.completeResult!,
          ...(mismatch === "hash" ? { resultHash: "0".repeat(64) } : { utf8ByteLength: 1 }),
        };
      const result = await f.deliver();
      expect(f.received.join("").length).toBe(4_096);
      expect(result?.completeResultPresentation).toBeUndefined();
    },
  );
  it.each(["rewrite", "truncate", "silent", "cancel"] as const)(
    "uses actual post-hook %s output, never pre-hook membership",
    async (mode) => {
      const replacement =
        mode === "rewrite" ? "replacement" : mode === "truncate" ? text.slice(0, 50) : "";
      const f = await fixture({
        hook: mode === "cancel" ? { cancel: true } : { content: replacement },
      });
      const result = await f.deliver();
      expect(f.received.join("")).toBe(mode === "cancel" || mode === "silent" ? "" : replacement);
      expect(result?.completeResultPresentation).toBeUndefined();
    },
  );
  it.each(["before", "hook"] as const)(
    "fences changed generation %s physical dispatch",
    async (when) => {
      const f = await fixture(
        when === "hook"
          ? {
              onHook: (child) => {
                child.generation = 3;
              },
            }
          : {},
      );
      if (when === "before") f.child.generation = 3;
      const result = await f.deliver();
      expect(f.received).toHaveLength(0);
      expect(result?.completeResultPresentation).toBeUndefined();
      expect(result?.delivered).not.toBe(true);
    },
  );
  it("never upgrades a missing-retention fallback to a complete result", async () => {
    const f = await fixture({ retained: false });
    const result = await f.deliver();
    expect(f.prepared.completeResult).toBeUndefined();
    expect(f.received.join("")).toContain("truncated-by-retention");
    expect(f.received.join("").length).toBeLessThanOrEqual(4_096);
    expect(result?.completeResultPresentation).toBeUndefined();
  });
  it("does not send a silent completion event", async () => {
    const f = await fixture();
    f.params.internalEvents![0].result = "NO_REPLY";
    const result = await f.deliver();
    expect(f.received).toHaveLength(0);
    expect(result?.completeResultPresentation).toBeUndefined();
  });
});
