import { beforeEach, describe, expect, it, vi } from "vitest";
import { createSubagentRunRecord } from "../subagent-test-fixtures.test-helpers.js";
import * as registryState from "../subagents/registry/subagent-registry-state.js";
import type { SubagentRunRecord } from "../subagents/registry/subagent-registry.types.js";
import { getSubagentRunIdentity } from "../subagents/registry/subagent-run-generation.js";
import { createSubagentsTool } from "./subagents-tool.js";
const owner = vi.hoisted(() => ({
  runs: [] as SubagentRunRecord[],
  listeners: new Set<() => void>(),
  cancel: vi.fn(),
}));
vi.mock("../subagents/registry/subagent-control.js", () => ({
  DEFAULT_RECENT_MINUTES: 30,
  MAX_RECENT_MINUTES: 1440,
  resolveSubagentController: () => ({
    controllerSessionKey: "agent:main:main",
    controllerAgentId: "main",
    callerSessionKey: "agent:main:main",
    callerIsSubagent: false,
    controlScope: "children",
  }),
  buildControlledSubagentRunsReadContext: async () => ({ list: {} }),
  killSubagentRunAdmin: owner.cancel,
}));
vi.mock("../subagents/registry/subagent-control-scope.js", async (importOriginal) => {
  const { resolveSubagentController, resolveSubagentControllerIdentity } =
    await importOriginal<typeof import("../subagents/registry/subagent-control-scope.js")>();
  return {
    resolveSubagentController,
    resolveSubagentControllerIdentity,
    ensureSubagentControllerOwnsRun: () => undefined,
    listControlledSubagentRunFacts: (key: string) =>
      owner.runs.filter((entry) => entry.requesterSessionKey === key),
  };
});
vi.mock("../subagents/registry/subagent-registry-state.js", () => ({
  getSubagentSessionListReadSnapshotIdentity: () => "ready",
  prepareSubagentSessionListReadCache: async () => {},
  prepareSubagentRunsSnapshotForRunIds: async () => ({
    consume: (read: (snapshot: ReadonlyMap<string, SubagentRunRecord>) => unknown) => ({
      ready: true,
      value: read(new Map(owner.runs.map((entry) => [entry.runId, entry]))),
    }),
  }),
}));
vi.mock("../subagents/registry/subagent-registry-publication.js", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../subagents/registry/subagent-registry-publication.js")>();
  return {
    ...actual,
    subscribeSubagentRunChanges: ((phase, listener) => {
      if (phase === "projection") {
        return actual.subscribeSubagentRunChanges(phase, listener);
      }
      const wake = () => listener({ runIds: undefined, sessionKeys: undefined });
      owner.listeners.add(wake);
      return () => owner.listeners.delete(wake);
    }) satisfies typeof actual.subscribeSubagentRunChanges,
  };
});
vi.mock("../subagents/registry/subagent-list.js", () => ({
  readSubagentListSessionEntries: () => new Map(),
  buildSubagentList: () => ({
    total: owner.runs.length,
    active: [],
    recent: [],
    text: "native subagents",
  }),
}));
beforeEach(() => {
  owner.runs = [];
  owner.listeners.clear();
  owner.cancel.mockReset();
});
function run() {
  const entry = createSubagentRunRecord({
    runId: "native-one",
    childSessionKey: "agent:main:subagent:one",
    requesterSessionKey: "agent:main:main",
    requesterAgentId: "main",
    generation: 1,
  });
  owner.runs = [entry];
  return entry;
}
function tool() {
  return createSubagentsTool({ config: {}, agentId: "main", agentSessionKey: "agent:main:main" });
}
describe("subagents native run contract", () => {
  it("subscribes before reading and does not consume the completed result's delivery obligation", async () => {
    const entry = run();
    entry.delivery = { status: "pending" };
    const pending = tool().execute("wait", { action: "wait", runIds: [entry.runId] });
    expect(owner.listeners.size).toBe(1);
    entry.execution = { status: "terminal", endedAt: 100, outcome: { status: "ok" } };
    for (const emit of owner.listeners) {
      emit();
    }
    expect((await pending).details).toMatchObject({
      reason: "completed",
      completed: [entry.runId],
      runs: [{ runId: entry.runId, deliveryStatus: "pending" }],
    });
    expect(owner.listeners.size).toBe(0);
    expect(entry.delivery.status).toBe("pending");
  });
  it("rechecks wait ownership without disclosing revoked results and reports suspended delivery", async () => {
    const entry = run();
    const pending = tool().execute("wait", { action: "wait", runIds: [entry.runId] });
    entry.requesterSessionKey = "agent:other:main";
    entry.execution = {
      status: "terminal",
      endedAt: 100,
      outcome: { status: "error", error: "FORMER_CHILD_PRIVATE_RESULT" },
    };
    for (const emit of owner.listeners) {
      emit();
    }
    const revoked = await pending;
    expect(revoked.details).toMatchObject({
      reason: "unavailable",
      unavailable: [entry.runId],
      runs: [],
    });
    expect(JSON.stringify(revoked.details)).not.toContain("FORMER_CHILD_PRIVATE_RESULT");
    entry.requesterSessionKey = "agent:main:main";
    entry.delivery = { status: "suspended" };
    expect(
      (
        await tool().execute("attention", {
          action: "wait",
          runIds: [entry.runId],
          timeoutSeconds: 0,
        })
      ).details,
    ).toMatchObject({ reason: "attention", attention: [entry.runId] });
    expect(owner.cancel).not.toHaveBeenCalled();
  });
  it("zero-timeout and abort do not cancel execution", async () => {
    const entry = run();
    expect(
      (await tool().execute("wait", { action: "wait", runIds: [entry.runId], timeoutSeconds: 0 }))
        .details,
    ).toMatchObject({ reason: "timeout" });
    expect(tool().parameters).toMatchObject({
      properties: {
        timeoutSeconds: {
          description: expect.stringMatching(/integer.*0–60.*default: 30.*0.*snapshot/),
        },
      },
    });
    const controller = new AbortController();
    const pending = tool().execute(
      "wait",
      { action: "wait", runIds: [entry.runId] },
      controller.signal,
    );
    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(owner.cancel).not.toHaveBeenCalled();
    expect(owner.listeners.size).toBe(0);
  });
  it("never lets a revoked controller selection cancel after an await", async () => {
    const entry = run();
    owner.cancel.mockImplementation(
      async (_params: unknown, control: { assertCurrent: () => void }) => {
        control.assertCurrent();
        owner.runs = [];
        await Promise.resolve();
        control.assertCurrent();
      },
    );
    await expect(
      tool().execute("cancel", { action: "cancel", runId: entry.runId }),
    ).rejects.toThrow("cancellation owner changed");
  });
});

function receiptRun(complete = true) {
  const entry = run();
  entry.requesterOrigin = { channel: "telegram", to: "123", accountId: "default" };
  entry.delivery = { status: "delivered", generation: 3 };
  const identity = getSubagentRunIdentity(entry);
  const result = {
    runId: entry.runId,
    childSessionKey: entry.childSessionKey,
    requesterSessionKey: entry.requesterSessionKey,
    runIdentity: identity,
    resultHash: "a".repeat(64),
    utf8ByteLength: 10,
  };
  entry.delivery.resultDelivery = {
    runIdentity: identity,
    deliveryGeneration: 3,
    acceptedParts: [
      {
        platformMessageId: "501",
        acceptance: {
          channel: "telegram",
          accountId: "default",
          to: "123",
          threadId: null,
          acceptedAtMs: 100,
        },
      },
    ],
    ...(complete
      ? {
          completeReceipt: {
            result,
            platformMessageIds: ["501"],
            acceptedAtMs: 100,
            scope: "complete",
          },
        }
      : {}),
  };
  return entry;
}

describe("bounded committed native delivery receipt list", () => {
  it("returns allowlisted complete metadata, not result bodies or private owner paths", async () => {
    const entry = receiptRun();
    Object.assign(entry.delivery!.resultDelivery!, {
      text: "PRIVATE_RESULT_BODY",
      secret: "PRIVATE_EXTRA_FIELD",
    });
    const result = await tool().execute("list", { action: "list", runIds: [entry.runId] });
    expect(result.details).toMatchObject({
      status: "ok",
      unavailable: [],
      receipts: [
        {
          runId: entry.runId,
          childSessionKey: entry.childSessionKey,
          ownerGeneration: 1,
          deliveryGeneration: 3,
          status: "complete",
          reportDelivery: "ACCEPTED_COMPLETE",
          resultHash: "a".repeat(64),
          channel: "telegram",
          accountId: "default",
          to: "123",
          threadId: null,
          platformMessageIds: ["501"],
          acceptedAtMs: 100,
        },
      ],
    });
    expect(JSON.stringify(result.details)).not.toMatch(
      /PRIVATE_RESULT_BODY|PRIVATE_EXTRA_FIELD|runIdentity|requesterStorePath/,
    );
  });
  it("retains positive partial evidence without claiming complete delivery", async () => {
    const entry = receiptRun(false);
    const result = await tool().execute("list", { action: "list", runIds: [entry.runId] });
    expect(result.details).toMatchObject({
      receipts: [{ status: "partial", reportDelivery: "UNKNOWN", platformMessageIds: ["501"] }],
    });
    expect(JSON.stringify(result.details)).not.toContain("resultHash");
  });
  it.each(["parent", "stale", "malformed", "account"] as const)(
    "keeps %s evidence UNKNOWN",
    async (mode) => {
      const entry = receiptRun();
      if (mode === "parent") delete entry.delivery!.resultDelivery;
      if (mode === "stale") entry.generation = 2;
      if (mode === "malformed") entry.delivery!.resultDelivery!.acceptedParts = [];
      if (mode === "account") entry.requesterOrigin!.accountId = "other";
      const result = await tool().execute("list", { action: "list", runIds: [entry.runId] });
      expect(result.details).toMatchObject({
        receipts: [{ status: "UNKNOWN", reportDelivery: "UNKNOWN" }],
      });
      expect(JSON.stringify(result.details)).not.toContain("platformMessageIds");
    },
  );
  it("does not truncate a large receipt into a false complete report", async () => {
    const entry = receiptRun(false);
    const part = entry.delivery!.resultDelivery!.acceptedParts[0];
    entry.delivery!.resultDelivery!.acceptedParts = Array.from({ length: 129 }, (_, index) => ({
      ...part,
      platformMessageId: String(index + 1),
    }));
    const result = await tool().execute("list", { action: "list", runIds: [entry.runId] });
    expect(result.details).toMatchObject({
      receipts: [{ status: "UNKNOWN", reason: "receipt_export_limit" }],
    });
  });
  it("never exports an out-of-scope child", async () => {
    const entry = receiptRun();
    entry.requesterSessionKey = "agent:other:main";
    const result = await tool().execute("list", { action: "list", runIds: [entry.runId] });
    expect(result.details).toMatchObject({ receipts: [], unavailable: [entry.runId] });
    expect(JSON.stringify(result.details)).not.toContain("501");
  });
  it("rechecks controller visibility after the worker read await", async () => {
    const entry = receiptRun();
    vi.spyOn(registryState, "prepareSubagentRunsSnapshotForRunIds").mockImplementationOnce(
      async () => {
        const snapshot = new Map([[entry.runId, entry]]);
        owner.runs = [];
        return { consume: (read) => ({ ready: true, value: read(snapshot) }) };
      },
    );
    const result = await tool().execute("list", { action: "list", runIds: [entry.runId] });
    expect(result.details).toMatchObject({ receipts: [], unavailable: [entry.runId] });
  });
  it("enforces the32run bound before receipt hydration", async () => {
    await expect(
      tool().execute("list", {
        action: "list",
        runIds: Array.from({ length: 33 }, (_, index) => String(index)),
      }),
    ).rejects.toThrow("At most 32");
  });
});
