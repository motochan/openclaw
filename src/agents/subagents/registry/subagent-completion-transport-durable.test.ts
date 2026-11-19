// External sender mocked; canonical SQLite owner and scoped list composition stay real.
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  openOpenClawStateDatabase,
  runOpenClawStateWriteTransaction,
} from "../../../state/openclaw-state-db.js";
import { withOpenClawTestState } from "../../../test-utils/openclaw-test-state.js";
import type { AgentInternalEvent } from "../../internal-events.js";
import { deliverCompletionDirect } from "../announce/subagent-announce-completion-delivery.js";
import {
  settleRequesterCompletionBatch,
  settleSubagentCompletionDelivery,
} from "../completion/subagent-completion-admission.store.js";
import {
  armRequesterWake,
  records,
} from "../completion/subagent-completion-admission.test-helpers.js";
import { buildSubagentList, captureSubagentListReadContext } from "./subagent-list.js";
import { subagentRuns } from "./subagent-registry-memory.js";
import {
  buildSubagentRunReadIndexFromRuns,
  listRunsForRequesterFromRuns,
} from "./subagent-registry-queries.js";
import * as custodyCodec from "./subagent-registry.store.codec.js";
import {
  rowToSubagentRunRecord,
  bindSubagentRunRecord,
  confirmCompletionTransportEvidenceReadback,
  retainCompletionTransportEvidence,
} from "./subagent-registry.store.codec.js";
import { upsertSubagentRunRowInDatabase } from "./subagent-registry.store.kernel.js";
import { readSubagentRun } from "./subagent-registry.store.sqlite.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

const sender = vi.hoisted(() => vi.fn());
vi.mock("../announce/subagent-announce-delivery.runtime.js", () => ({
  sendSubagentAnnounceMessage: sender,
  tryResolveSubagentRequesterAgentId: () => "main",
  dispatchSubagentAnnounceAgent: vi.fn(),
}));
afterEach(() => {
  sender.mockReset();
  subagentRuns.clear();
});
const requester = "agent:main:telegram:direct:123";
function run(id = "selected"): SubagentRunRecord {
  return {
    runId: id,
    childSessionKey: "agent:main:subagent:" + id,
    requesterSessionKey: requester,
    requesterDisplayKey: requester,
    requesterOrigin: { channel: "telegram", to: "123" },
    task: "same label",
    label: "same label",
    cleanup: "keep",
    createdAt: Date.now() - 100,
    expectsCompletionMessage: true,
    execution: { status: "terminal", endedAt: Date.now(), outcome: { status: "ok" } },
    completion: { required: true },
    delivery: { status: "delivered" },
  };
}
function event(entry: SubagentRunRecord): AgentInternalEvent {
  return {
    type: "task_completion",
    source: "subagent",
    childRunId: entry.runId,
    childSessionKey: entry.childSessionKey,
    announceType: "task completion",
    taskLabel: "same label",
    status: "ok",
    statusLabel: "completed",
    result: "synthetic result",
    replyInstruction: "report",
  };
}
async function produce(entry: SubagentRunRecord, partial = false, events = [event(entry)]) {
  sender.mockImplementationOnce(async (params) => {
    params.onDeliveryResult({ channel: "telegram", messageId: "platform-first" });
    if (partial) throw new Error("second part failed");
    params.onDeliveryResult({ channel: "telegram", messageId: "platform-last" });
    return { deliveryStatus: "sent" };
  });
  return (await deliverCompletionDirect({
    cfg: {},
    requesterSessionKey: requester,
    deliveryTarget: { deliver: true, channel: "telegram", to: "123" },
    internalEvents: events,
    directIdempotencyKey: "synthetic",
    contentKind: "completed_result",
  }))!;
}
function list(runs: SubagentRunRecord[], all = new Map(runs.map((entry) => [entry.runId, entry]))) {
  const index = buildSubagentRunReadIndexFromRuns({ runs: all });
  return buildSubagentList({
    context: captureSubagentListReadContext(runs, index, all, 30),
    sessionEntries: new Map(),
  });
}
async function roundtrip(entry: SubagentRunRecord) {
  const db = openOpenClawStateDatabase();
  runOpenClawStateWriteTransaction(
    (database) => upsertSubagentRunRowInDatabase(database, bindSubagentRunRecord(entry)),
    { database: db },
  );
  const stored = readSubagentRun(db, entry.runId)!;
  confirmCompletionTransportEvidenceReadback(entry, stored);
  return stored;
}
describe("direct completion durable scoped custody", () => {
  it("projects exact completed send only after canonical commit/readback", async () => {
    await withOpenClawTestState({ label: "transport-complete" }, async () => {
      const entry = run();
      const outcome = await produce(entry);
      expect(retainCompletionTransportEvidence(entry, outcome)).toBe(true);
      expect(list([entry]).recent[0].completionTransportStatus).toBe("UNKNOWN");
      const stored = await roundtrip(entry);
      expect(list([stored]).recent[0]).toMatchObject({
        completionTransportStatus: "DELIVERED",
        completionTransportEvidence: {
          childRunId: entry.runId,
          platformMessageIds: ["platform-first", "platform-last"],
          durable: true,
        },
      });
      expect(
        JSON.parse(bindSubagentRunRecord(entry).payload_json).delivery.completionTransportEvidence
          .durable,
      ).toBeUndefined();
    });
  });
  it("retains first-part IDs after send failure but never calls partial complete", async () => {
    await withOpenClawTestState({ label: "transport-partial" }, async () => {
      const entry = run();
      retainCompletionTransportEvidence(entry, await produce(entry, true));
      const stored = await roundtrip(entry);
      expect(list([stored]).recent[0]).toMatchObject({
        completionTransportStatus: "UNKNOWN",
        completionTransportEvidence: {
          scope: "partial",
          platformMessageIds: ["platform-first"],
          durable: true,
        },
      });
    });
  });
  it("binds the selected same-label event, not another run", async () => {
    const entry = run();
    const other = run("other");
    const outcome = await produce(entry, false, [event(entry), event(other)]);
    expect(retainCompletionTransportEvidence(entry, outcome)).toBe(false);
    expect(retainCompletionTransportEvidence(other, outcome)).toBe(true);
    expect(list([entry]).recent[0].completionTransportStatus).toBe("UNKNOWN");
  });
  it("keeps older canonical rows UNKNOWN", async () => {
    await withOpenClawTestState({ label: "transport-older" }, async () => {
      expect(list([await roundtrip(run())]).recent[0]).toMatchObject({
        completionTransportStatus: "UNKNOWN",
      });
    });
  });
  it("rejects wrong requester and wrong route even with platform IDs", async () => {
    const entry = run();
    const outcome = await produce(entry);
    const evidence = outcome.completionTransportEvidence!;
    for (const patch of [
      { requesterSessionKey: "other" },
      { to: "456" },
      { childRunId: "other" },
    ]) {
      expect(
        retainCompletionTransportEvidence(entry, {
          ...outcome,
          completionTransportEvidence: { ...evidence, ...patch },
        }),
      ).toBe(false);
    }
  });
  it("does not mark tentative evidence durable on absent readback", async () => {
    const entry = run();
    retainCompletionTransportEvidence(entry, await produce(entry));
    confirmCompletionTransportEvidenceReadback(entry, null);
    expect(list([entry]).recent[0].completionTransportStatus).toBe("UNKNOWN");
  });
  it("preserves requester visibility and allowlists metadata", async () => {
    await withOpenClawTestState({ label: "transport-scope" }, async () => {
      const entry = run();
      retainCompletionTransportEvidence(entry, await produce(entry));
      const stored = await roundtrip(entry);
      const hidden = run("hidden");
      hidden.requesterSessionKey = "agent:other:main";
      Object.assign(stored.delivery!.completionTransportEvidence!, {
        body: "must not project",
        credential: "synthetic-not-secret",
      });
      const all = new Map([stored, hidden].map((item) => [item.runId, item]));
      const visible = listRunsForRequesterFromRuns(all, requester);
      const result = list(visible, all);
      expect(result.recent.map((item) => item.runId)).toEqual([stored.runId]);
      expect(result.recent[0].completionTransportEvidence).not.toHaveProperty("body");
      expect(result.recent[0].completionTransportEvidence).not.toHaveProperty("credential");
    });
  });
  it("settles a real frozen cohort without spreading the selected receipt", async () => {
    await withOpenClawTestState({ label: "transport-cohort" }, async () => {
      const db = openOpenClawStateDatabase();
      const inputs = [records(), records()];
      for (const [i, input] of inputs.entries()) {
        input.subagent.runId = "cohort-" + i;
        input.subagent.childSessionKey += i;
        input.subagent.requesterSessionKey = requester;
        input.subagent.requesterOrigin = { channel: "telegram", to: "123" };
        input.task.taskId += i;
        input.task.runId += i;
        input.task.childSessionKey = input.subagent.childSessionKey;
        input.task.requesterSessionKey = requester;
        input.subagent.taskRunId = input.task.runId;
        armRequesterWake(input, ["cohort-0", "cohort-1"]);
        settleSubagentCompletionDelivery({
          subagent: input.subagent,
          task: input.task,
          databaseOptions: { database: db },
        });
        subagentRuns.set(input.subagent.runId, input.subagent);
      }
      const outcome = await produce(inputs[1].subagent);
      settleRequesterCompletionBatch({
        entries: inputs.map(({ subagent, task }) => ({ subagent, taskId: task.taskId })),
        outcome,
        isCurrent: () => true,
        databaseOptions: { database: db },
      });
      expect(
        readSubagentRun(db, "cohort-0")!.delivery!.completionTransportEvidence,
      ).toBeUndefined();
      expect(readSubagentRun(db, "cohort-1")!.delivery!.completionTransportEvidence).toMatchObject({
        childRunId: "cohort-1",
        scope: "complete",
        durable: true,
      });
    });
  });
});

describe("optional custody cannot erase committed domain state", () => {
  it.each(["wrong-route", "invalid-id"])("drops malformed optional receipt: %s", async (fault) => {
    const entry = run();
    retainCompletionTransportEvidence(entry, await produce(entry));
    const row = bindSubagentRunRecord(entry);
    const payload = JSON.parse(row.payload_json);
    if (fault === "wrong-route") payload.delivery.completionTransportEvidence.to = "other";
    else payload.delivery.completionTransportEvidence.platformMessageIds = ["unknown"];
    const decoded = rowToSubagentRunRecord({ ...row, payload_json: JSON.stringify(payload) });
    expect(decoded?.runId).toBe(entry.runId);
    expect(decoded?.delivery?.completionTransportEvidence).toBeUndefined();
    expect(list([decoded!]).recent[0].completionTransportStatus).toBe("UNKNOWN");
  });
});

describe("committed batch publication under diagnostic fault", () => {
  it("publishes committed registry domain state when optional custody confirmation throws", async () => {
    await withOpenClawTestState({ label: "custody-postcommit-fault" }, async () => {
      const db = openOpenClawStateDatabase();
      const input = records();
      input.subagent.requesterSessionKey = requester;
      input.subagent.requesterOrigin = { channel: "telegram", to: "123" };
      input.task.requesterSessionKey = requester;
      armRequesterWake(input, [input.subagent.runId]);
      settleSubagentCompletionDelivery({
        subagent: input.subagent,
        task: input.task,
        databaseOptions: { database: db },
      });
      subagentRuns.set(input.subagent.runId, input.subagent);
      const outcome = await produce(input.subagent);
      const diagnostic = vi
        .spyOn(custodyCodec, "publishCommittedCompletionTransportEvidence")
        .mockImplementation(() => {
          throw new Error("optional confirmation unavailable after COMMIT");
        });
      try {
        expect(() =>
          settleRequesterCompletionBatch({
            entries: [{ subagent: input.subagent, taskId: input.task.taskId }],
            outcome,
            isCurrent: () => true,
            databaseOptions: { database: db },
          }),
        ).not.toThrow();
        expect(subagentRuns.get(input.subagent.runId)?.delivery?.status).toBe("delivered");
        expect(
          subagentRuns.get(input.subagent.runId)?.delivery?.completionTransportEvidence?.durable,
        ).toBeUndefined();
        expect(
          list([subagentRuns.get(input.subagent.runId)!]).recent[0].completionTransportStatus,
        ).toBe("UNKNOWN");
        expect(readSubagentRun(db, input.subagent.runId)?.delivery?.status).toBe("delivered");
      } finally {
        diagnostic.mockRestore();
      }
    });
  });
});
