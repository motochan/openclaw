import { afterEach, describe, expect, it } from "vitest";
import { sendMessageTelegram } from "../../../../extensions/telegram/runtime-api.js";
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
    native?:
      | "sent"
      | "partial"
      | "observer"
      | "recipient"
      | "thread"
      | "missing-id"
      | "silent-tail"
      | "observer-first"
      | "observer-tail"
      | "unknown-coverage";
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
  let physicalCalls = 0;
  let terminalCoverage: "complete" | "partial" | undefined;
  const plugin = createOutboundTestPlugin({
    id: "telegram",
    outbound: {
      deliveryMode: "direct",
      chunker: chunkText,
      textChunkLimit: options.native ? 100_000 : 2_000,
      sendText: async ({ text: chunk, onDeliveryResult }) => {
        if (options.native) {
          const sent = await sendMessageTelegram("12345", chunk, {
            cfg: { channels: { telegram: { botToken: "123456:fixture", richMessages: false } } },
            accountId: "default",
            textMode: "html",
            api: {
              sendMessage: async (_chatId, physicalText) => {
                physicalCalls++;
                if (options.native === "silent-tail" && received.length === 1)
                  throw new Error("Bad Request: message text is empty");
                if (options.native === "partial" && received.length === 1)
                  throw new Error("physical second part rejected");
                received.push(physicalText);
                return {
                  message_id:
                    options.native === "missing-id" && received.length === 2
                      ? Number.NaN
                      : received.length,
                  date: 1,
                  chat: {
                    id: options.native === "recipient" ? 456 : 12345,
                    type: "private" as const,
                    first_name: "fixture",
                  },
                  text: physicalText,
                  ...(options.native === "thread" ? { message_thread_id: 77 } : {}),
                };
              },
            },
            onDeliveryResult: async (delivery) => {
              if (
                options.native === "observer-first" ||
                (options.native === "observer-tail" && received.length === 2)
              )
                throw new Error("observer failed before forwarding");
              await onDeliveryResult?.({
                channel: "telegram",
                messageId: delivery.messageId,
                receipt: delivery.receipt,
                target: { kind: "chat", id: delivery.chatId },
              });
              if (options.native === "observer" && received.length === 2)
                throw new Error("physical observer failed after accept");
            },
          });
          terminalCoverage = sent.receipt?.textCoverage;
          if (options.native === "unknown-coverage" && sent.receipt)
            delete sent.receipt.textCoverage;
          return {
            channel: "telegram",
            messageId: sent.messageId,
            receipt: sent.receipt,
            target: { kind: "chat", id: sent.chatId },
          };
        }
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
    deliveryTarget: { deliver: true, channel: "telegram", to: "12345", accountId: "default" },
    internalEvents: taskCompletionEvents({
      childSessionKey: child.childSessionKey,
      result: prepared.text ?? "NO_REPLY",
    }),
    contentKind: "completed_result",
    sourceRunId: child.runId,
    sourceSessionKey: child.childSessionKey,
    preparedResult: prepared,
  };
  const completionEvent = params.internalEvents?.[0];
  if (!completionEvent) throw new Error("Fixture requires one completion event");
  return {
    child,
    prepared,
    received,
    params,
    completionEvent,
    physicalCalls: () => physicalCalls,
    terminalCoverage: () => terminalCoverage,
    deliver: () => deliverCompletionDirect(params),
  };
}

describe("complete direct result presentation", () => {
  it("does not credit an empty-rejected required tail of the native Telegram plan", async () => {
    const f = await fixture({ native: "silent-tail" });
    const result = await f.deliver();
    expect(f.physicalCalls()).toBe(3); // initial HTML success, tail HTML/plain rejections
    expect(f.received).toHaveLength(1);
    expect(f.received.join("").length).toBe(4_000);
    expect(text.length).toBe(5_002);
    expect(f.terminalCoverage()).toBe("partial");
    expect(result).toMatchObject({
      delivered: false,
      terminal: true,
      disposition: "permanent_failure",
    });
    expect(result?.acceptedResultParts?.map((part) => part.platformMessageId)).toEqual(["1"]);
    expect(result?.completeResultReceipt).toBeUndefined();
  });
  it.each(["observer-first", "observer-tail"] as const)(
    "retains native acceptance when %s fails before forwarding",
    async (native) => {
      const f = await fixture({ native });
      const result = await f.deliver();
      const count = native === "observer-first" ? 1 : 2;
      expect(f.physicalCalls()).toBe(count);
      expect(f.received).toHaveLength(count);
      expect(result).toMatchObject({
        delivered: false,
        terminal: true,
        disposition: "permanent_failure",
      });
      expect(result?.acceptedResultParts?.map((part) => part.platformMessageId)).toEqual(
        count === 1 ? ["1"] : ["1", "2"],
      );
      expect(result?.completeResultReceipt).toBeUndefined();
    },
  );
  it("does not treat missing terminal native plan coverage as complete", async () => {
    const f = await fixture({ native: "unknown-coverage" });
    const result = await f.deliver();
    expect(f.received.join("")).toBe(text);
    expect(result?.completeResultReceipt).toBeUndefined();
    expect(result?.acceptedResultParts).toHaveLength(2);
  });
  it("does not infer a missing account on recovered pre-forward acceptance", async () => {
    const f = await fixture({ native: "observer-first" });
    delete f.params.deliveryTarget.accountId;
    const result = await f.deliver();
    expect(f.physicalCalls()).toBe(1);
    expect(result).toMatchObject({ delivered: false, terminal: true });
    expect(result?.acceptedResultParts).toBeUndefined();
    expect(result?.completeResultReceipt).toBeUndefined();
  });

  it("joins actual Telegram API accepted parts to the exact complete presentation", async () => {
    const f = await fixture({ native: "sent" });
    const result = await f.deliver();
    expect(f.received).toHaveLength(2);
    expect(f.received.join("")).toBe(text);
    expect(result?.completeResultReceipt).toMatchObject({
      scope: "complete",
      platformMessageIds: ["1", "2"],
      result: f.prepared.completeResult,
    });
    expect(result?.acceptedResultParts).toHaveLength(2);
    for (const part of result?.acceptedResultParts ?? []) {
      expect(part.acceptance).toMatchObject({
        channel: "telegram",
        accountId: "default",
        to: "12345",
        threadId: null,
        acceptedAtMs: expect.any(Number),
      });
      expect(Object.isFrozen(part.acceptance)).toBe(true);
      expect(part).not.toHaveProperty("raw");
      expect(part).not.toHaveProperty("text");
    }
  });
  it.each(["partial", "observer", "recipient", "thread", "missing-id"] as const)(
    "never promotes actual Telegram %s acceptance to a complete result",
    async (native) => {
      const f = await fixture({ native });
      const result = await f.deliver();
      expect(f.received.length).toBeGreaterThan(0);
      expect(result?.completeResultReceipt).toBeUndefined();
      if (native === "partial" || native === "observer" || native === "missing-id") {
        expect(result?.acceptedResultParts?.map((part) => part.platformMessageId)).toEqual(
          native === "observer" ? ["1", "2"] : ["1"],
        );
        expect(result?.delivered).not.toBe(true);
      }
    },
  );

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
        f.completionEvent.childSessionKey = "agent:main:subagent:other";
      else {
        const identity = f.prepared.completeResult;
        if (!identity) throw new Error("Fixture requires complete retained identity");
        f.prepared.completeResult = {
          ...identity,
          ...(mismatch === "hash" ? { resultHash: "0".repeat(64) } : { utf8ByteLength: 1 }),
        };
      }
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
    f.completionEvent.result = "NO_REPLY";
    const result = await f.deliver();
    expect(f.received).toHaveLength(0);
    expect(result?.completeResultPresentation).toBeUndefined();
  });
});
