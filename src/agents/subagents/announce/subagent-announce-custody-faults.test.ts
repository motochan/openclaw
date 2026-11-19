// Controller -> registered announce -> real completion producer seam coverage.
// Internal delivery orchestration, registry lookups, and outbound multipart are mocked;
// this does not qualify full production routing or the real outbound implementation.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { normalizeSessionDeliveryState } from "../../../utils/delivery-context.shared.js";
import type { EmbeddedAgentQueueMessageOutcome } from "../../embedded-agent-runner/runs.js";
import type { SubagentRunRecord } from "../registry/subagent-registry.types.js";
import { createSubagentAnnounceDeliveryRuntimeMock } from "./subagent-announce.test-support.js";

type AgentCallRequest = { method?: string; params?: Record<string, unknown> };
type AgentCallResponse = {
  runId?: string;
  status: string;
  error?: string;
  disposition?: "ambiguous";
};

const agentSpy = vi.fn(async (_req: AgentCallRequest): Promise<AgentCallResponse> => ({
  runId: "run-main",
  status: "ok",
}));
const sessionsDeleteSpy = vi.fn((_req: AgentCallRequest) => undefined);
const callGatewayMock = vi.fn(async (_request: unknown) => ({}));
const loadSessionStoreMock = vi.fn((_storePath: string) => ({}));
const resolveAgentIdFromSessionKeyMock = vi.fn<
  typeof import("./subagent-announce.runtime.js").resolveAgentIdFromSessionKey
>((sessionKey, configuredDefaultAgentId) => {
  return sessionKey?.match(/^agent:([^:]+)/)?.[1] ?? configuredDefaultAgentId ?? "main";
});
const resolveStorePathMock = vi.fn((_store: unknown, _options: unknown) => "/tmp/sessions.json");
const resolveMainSessionKeyMock = vi.fn((_cfg: unknown) => "agent:main:main");
const isEmbeddedAgentRunActiveMock = vi.fn((_sessionId: string) => false);
const queueEmbeddedAgentMessageWithOutcomeMock = vi.fn(
  (sessionId: string, _text: string, _options?: unknown): EmbeddedAgentQueueMessageOutcome => ({
    queued: false,
    sessionId,
    reason: "not_streaming" as const,
    gatewayHealth: "live" as const,
  }),
);
const waitForEmbeddedAgentRunEndMock = vi.fn(
  async (_sessionId: string, _timeoutMs?: number) => true,
);
let mockConfig: ReturnType<(typeof import("../../../config/config.js"))["getRuntimeConfig"]> = {
  session: {
    mainKey: "main",
    scope: "per-sender",
  },
};

const { subagentRegistryRuntimeMock } = vi.hoisted(() => ({
  subagentRegistryRuntimeMock: {
    shouldIgnorePostCompletionAnnounceForSession: vi.fn(() => false),
    isSubagentSessionRunActive: vi.fn(() => true),
    countActiveDescendantRuns: vi.fn(() => 0),
    countPendingDescendantRuns: vi.fn(() => 0),
    hasDescendantRunAwaitingSettle: vi.fn(() => false),
    getLatestSubagentRunByChildSessionKey: vi.fn(() => undefined),
    listSubagentRunsForRequester: vi.fn<() => SubagentRunRecord[]>(() => []),
    replaceSubagentRunAfterSteer: vi.fn(() => true),
    resolveRequesterForChildSession: vi.fn(() => null),
  },
}));

vi.mock("./subagent-announce.runtime.js", () => ({
  callSubagentLifecycleGateway: (request: unknown) => callGatewayMock(request),
  dispatchGatewayMethodInProcess: (
    method: string,
    params: Record<string, unknown>,
    options?: { timeoutMs?: number },
  ) => callGatewayMock({ method, params, timeoutMs: options?.timeoutMs }),
  isEmbeddedAgentRunActive: (sessionId: string) => isEmbeddedAgentRunActiveMock(sessionId),
  getRuntimeConfig: () => mockConfig,
  loadSessionStore: (storePath: string) => loadSessionStoreMock(storePath),
  readSessionMessagesAsync: vi.fn(async () => []),
  readSubagentSessionEntry: (storePath: string, sessionKey: string) =>
    (loadSessionStoreMock(storePath) as Record<string, unknown>)[sessionKey],
  resolveAgentIdFromSessionKey: (sessionKey: string) =>
    resolveAgentIdFromSessionKeyMock(sessionKey),
  resolveMainSessionKey: (cfg: unknown) => resolveMainSessionKeyMock(cfg),
  resolveSessionStorePathCore: (store: unknown, options: unknown) =>
    resolveStorePathMock(store, options),
  waitForEmbeddedAgentRunEnd: (sessionId: string, timeoutMs?: number) =>
    waitForEmbeddedAgentRunEndMock(sessionId, timeoutMs),
}));

vi.mock("./subagent-announce-delivery.runtime.js", () => ({
  ...createSubagentAnnounceDeliveryRuntimeMock({
    callGateway: (request: unknown) => callGatewayMock(request),
    getRuntimeConfig: () => mockConfig,
    loadSessionStore: (storePath: string) => loadSessionStoreMock(storePath),
    resolveAgentIdFromSessionKey: (sessionKey: string) =>
      resolveAgentIdFromSessionKeyMock(sessionKey),
    resolveMainSessionKey: (cfg: unknown) => resolveMainSessionKeyMock(cfg),
    resolveSessionStorePathCore: (store: unknown, options: unknown) =>
      resolveStorePathMock(store, options),
    isEmbeddedAgentRunActive: (sessionId: string) => isEmbeddedAgentRunActiveMock(sessionId),
    queueEmbeddedAgentMessageWithOutcome: (sessionId: string, text: string, options?: unknown) =>
      queueEmbeddedAgentMessageWithOutcomeMock(sessionId, text, options),
  }),
  tryResolveSubagentRequesterAgentId: () => "main",
  sendSubagentAnnounceMessage: (...args: Parameters<typeof sendMock>) => sendMock(...args),
}));

vi.mock("./subagent-announce-delivery.js", () => ({
  deliverSubagentAnnouncement: async (
    params: Parameters<
      typeof import("./subagent-announce-delivery.js").deliverSubagentAnnouncement
    >[0],
  ) => {
    const events = wrongRun
      ? params.internalEvents?.map((event) => ({ ...event, childRunId: "other-same-label" }))
      : params.internalEvents;
    return (await deliverCompletionDirect({
      cfg: {},
      requesterSessionKey: params.requesterSessionKey,
      deliveryTarget: { deliver: true, channel: "telegram", to: "123" },
      internalEvents: events,
      directIdempotencyKey: "custody-fixture",
      contentKind: "completed_result",
      onDeliveryResult: params.onDeliveryResult,
    }))!;
  },
  loadRequesterSessionEntry: (sessionKey: string) => {
    const store = loadSessionStoreMock("/tmp/sessions.json") as Record<string, unknown>;
    const entry = store?.[sessionKey];
    return { entry };
  },
  loadSessionEntryByKey: (sessionKey: string) => {
    const store = loadSessionStoreMock("/tmp/sessions.json") as Record<string, unknown>;
    return store?.[sessionKey] ?? { sessionId: sessionKey };
  },
  resolveAnnounceOrigin: (
    entry:
      | {
          lastChannel?: string;
          lastTo?: string;
          lastAccountId?: string;
          lastThreadId?: string;
          origin?: { provider?: string; channel?: string; accountId?: string };
        }
      | undefined,
    requesterOrigin?: { channel?: string; to?: string; accountId?: string; threadId?: string },
  ) => ({
    channel:
      requesterOrigin?.channel ??
      entry?.lastChannel ??
      entry?.origin?.provider ??
      entry?.origin?.channel,
    to: requesterOrigin?.to ?? entry?.lastTo,
    accountId: requesterOrigin?.accountId ?? entry?.lastAccountId ?? entry?.origin?.accountId,
    threadId: requesterOrigin?.threadId ?? entry?.lastThreadId,
  }),
  resolveSubagentCompletionOrigin: async (params: { requesterOrigin?: unknown }) =>
    params.requesterOrigin,
  resolveSubagentAnnounceTimeoutMs: () => 10_000,
  runAnnounceDeliveryWithRetry: async <T>(params: { run: () => Promise<T> }) => await params.run(),
}));

vi.mock("../registry/subagent-registry-read.js", () => subagentRegistryRuntimeMock);
vi.mock("../registry/subagent-registry-runtime.js", () => subagentRegistryRuntimeMock);
import { defaultRuntime } from "../../../runtime.js";
import { withOpenClawTestState } from "../../../test-utils/openclaw-test-state.js";
import {
  SubagentLifecycleController,
  type SubagentLifecycleOptions,
} from "../registry/subagent-registry-lifecycle.js";
import { deliverCompletionDirect } from "./subagent-announce-completion-delivery.js";
import { applySubagentWaitOutcome } from "./subagent-announce-output.js";
import { testing as outputTesting } from "./subagent-announce-output.test-support.js";
import { runSubagentAnnounceFlow } from "./subagent-announce.js";
const sendMock =
  vi.fn<typeof import("./subagent-announce-delivery.runtime.js").sendSubagentAnnounceMessage>();
let wrongRun = false;

describe("registered announce cleanup custody faults", () => {
  it.each(["persist-fails", "complete", "wrong-run"] as const)(
    "continues multipart owner: %s",
    async (mode) => {
      await withOpenClawTestState({ label: "registered-custody-" + mode }, async () => {
        wrongRun = mode === "wrong-run";
        const entry: SubagentRunRecord = {
          runId: "selected",
          childSessionKey: "agent:main:subagent:selected",
          requesterSessionKey: "agent:main:telegram:direct:123",
          requesterDisplayKey: "main",
          requesterOrigin: { channel: "telegram", to: "123" },
          task: "same label",
          label: "same label",
          cleanup: "keep",
          retainAttachmentsOnKeep: true,
          createdAt: Date.now() - 100,
          expectsCompletionMessage: true,
          execution: { status: "terminal", endedAt: Date.now(), outcome: { status: "ok" } },
          completion: { required: true, resultText: "synthetic complete result" },
          delivery: { status: "pending" },
        };
        const warn = vi.fn();
        let finish!: () => void;
        const finished = new Promise<void>((resolve) => {
          finish = resolve;
        });
        const persistenceError = new Error("first callback persistence unavailable");
        const persistDeliveryAsync =
          mode === "persist-fails"
            ? vi.fn(async () => {
                throw persistenceError;
              })
            : undefined;
        const parts: string[] = [];
        sendMock.mockImplementation(async (params) => {
          parts.push("first");
          await params.onDeliveryResult?.({ channel: "telegram", messageId: "part-1" });
          await Promise.resolve();
          parts.push("last");
          await params.onDeliveryResult?.({ channel: "telegram", messageId: "part-2" });
          return { deliveryStatus: "sent" };
        });
        const options: SubagentLifecycleOptions = {
          runs: new Map([[entry.runId, entry]]),
          resumedRuns: new Set(),
          subagentAnnounceTimeoutMs: 1000,
          getRuntimeConfig: () => ({}),
          persist: () => {},
          persistOrThrow: () => {
            if (entry.cleanupCompletedAt !== undefined) finish();
          },
          persistDeliveryAsync,
          clearPendingLifecycleError: () => {},
          countPendingDescendantRuns: () => 0,
          getLatestRunForChildSession: () => null,
          suppressAnnounceForSteerRestart: () => false,
          resolveSubagentTask: () => ({ lookup: "available" }),
          shouldEmitEndedHookForRun: () => false,
          emitSubagentEndedHookForRun: async () => {},
          emitSubagentProgressEndedForRun: async () => {},
          notifyContextEngineSubagentEnded: async () => {},
          retireSupersededRun: async () => {},
          resumeSubagentRun: () => {},
          callGateway: async <T>() => ({}) as T,
          captureSubagentCompletionReply: async () => "synthetic complete result",
          cleanupBrowserSessionsForLifecycleEnd: async () => {},
          runSubagentAnnounceFlow,
          maybeWakeRequesterAfterAllChildrenSettled: async () => false,
          warn,
        };
        const controller = new SubagentLifecycleController(options);
        expect(controller.startSubagentAnnounceCleanupFlow(entry.runId, entry)).toBe(true);
        await finished;
        expect(parts).toEqual(["first", "last"]);
        const evidence = entry.delivery?.completionTransportEvidence;
        if (mode === "complete") {
          expect(evidence).toMatchObject({
            childRunId: "selected",
            scope: "complete",
            durable: true,
            platformMessageIds: ["part-1", "part-2"],
          });
        } else {
          expect(evidence?.durable).toBeUndefined();
          if (mode === "wrong-run") expect(evidence).toBeUndefined();
          else {
            expect(persistDeliveryAsync).toHaveBeenCalled();
            expect(warn).toHaveBeenCalledWith(
              "Completion transport custody UNKNOWN",
              expect.objectContaining({ runId: "selected", error: persistenceError }),
            );
          }
        }
      });
    },
  );
});
