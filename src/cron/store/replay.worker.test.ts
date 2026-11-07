import { afterAll, beforeAll, expect, it } from "vitest";
import { loseFirstCronMutationReply } from "../../../test/helpers/cron/runtime-mutation.js";
import { createSqliteWorkerWriteAdmission } from "../../infra/sqlite-worker-store.js";
import {
  closeOpenClawStateDatabaseAsync,
  openOpenClawStateDatabase,
} from "../../state/openclaw-state-db.js";
import { captureOpenClawStateWorkerContext } from "../../state/openclaw-state-worker-context.js";
import type { OpenClawStateWorkerContext } from "../../state/openclaw-state-worker-context.types.js";
import type { OpenClawStateWorkerOperations } from "../../state/openclaw-state-worker-contract.js";
import { runOpenClawStateWorkerOperation } from "../../state/openclaw-state-worker-store.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../../test-utils/openclaw-test-state.js";
import { resolveCronJobConfigRevision } from "../config-revision.js";
import { cronReplayIntentHash, cronReplayRunId, type CronReplayRequest } from "../replay.js";
import { saveCronStore } from "../store.js";
import type { CronJob } from "../types.js";

const now = 1_800_000_000_000;
let fixture: OpenClawTestState;
let context: OpenClawStateWorkerContext;

beforeAll(async () => {
  fixture = await createOpenClawTestState({ label: "cron-native-replay" });
  context = captureOpenClawStateWorkerContext();
  await execute("cron.initializeRunReceipts", {});
});
afterAll(async () => {
  await fixture?.cleanup();
});

function execute<Key extends keyof OpenClawStateWorkerOperations>(
  type: Key,
  input: OpenClawStateWorkerOperations[Key]["input"],
  beforeCommit?: () => void,
) {
  const guard = () => context.admission.assertCurrent();
  const standard = createSqliteWorkerWriteAdmission(
    (request) => {
      guard();
      if (request.stage === "commit") {
        beforeCommit?.();
      }
    },
    [context.admission.databasePath],
  );
  return runOpenClawStateWorkerOperation(context, (scope) => scope.execute({ type, input }), {
    assertCurrent: guard,
    createAdmission: standard,
  });
}

async function setup(label: string) {
  const storeKey = fixture.statePath("cron", `${label}.json`);
  const job: CronJob = {
    id: label,
    name: label,
    agentId: "main",
    enabled: true,
    createdAtMs: now - 1000,
    updatedAtMs: now - 1000,
    schedule: { kind: "every", everyMs: 60_000 },
    sessionTarget: "isolated",
    wakeMode: "next-heartbeat",
    payload: { kind: "command", argv: ["echo", "synthetic"] },
    delivery: { mode: "none" },
    state: { nextRunAtMs: now },
  };
  await saveCronStore(storeKey, { version: 1, jobs: [job] });
  const replay: CronReplayRequest = {
    ref: { operationId: label, effectId: "run", attempt: 1 },
    authorityId: "synthetic-operator",
    intent: { action: "run", jobId: label, mode: "force" },
  };
  const admit = (request = replay) =>
    execute("cron.replayAdmit", {
      storeKey,
      replay: request,
      maxAttempts: 2,
      evidence: "reviewed fixture",
      nowMs: now,
    });
  const submit = (request = replay, beforeCommit?: () => void) =>
    execute(
      "cron.requestRuns",
      {
        storeKey,
        nowMs: now,
        requests: [
          {
            jobId: job.id,
            configRevision: resolveCronJobConfigRevision(job),
            mode: "force",
            receiptId: cronReplayRunId(storeKey, request.ref),
            requestRunId: cronReplayRunId(storeKey, request.ref),
            replay: request,
          },
        ],
      },
      beforeCommit,
    );
  const inspect = (request = replay) =>
    execute("cron.replayInspect", { storeKey, replay: request });
  return { storeKey, job, replay, admit, submit, inspect };
}

it("commits one run and returns its original receipt for concurrent native submissions", async () => {
  const target = await setup("race");
  await target.admit();
  const results = await Promise.all([target.submit(), target.submit(), target.submit()]);
  expect(results.reduce((sum, result) => sum + result.accepted.length, 0)).toBe(1);
  const expected = {
    state: "accepted",
    result: {
      ok: true,
      enqueued: true,
      runId: cronReplayRunId(target.storeKey, target.replay.ref),
    },
  };
  expect(results.flatMap((result) => result.replayed ?? [])).toEqual([expected, expected]);
  expect(await target.inspect()).toEqual(expected);
  await execute("cron.cancelRequests", {
    storeKey: target.storeKey,
    receiptIds: [expected.result.runId],
    nowMs: now + 1,
    reason: "fixture completion",
  });
  expect((await target.submit()).replayed).toEqual([expected]);
  expect((await target.submit()).accepted).toEqual([]);
  // Equal intent is a legitimate second action only after separate host admission.
  const nextStep = { ...target.replay, ref: { ...target.replay.ref, effectId: "second-run" } };
  await target.admit(nextStep);
  expect((await target.submit(nextStep)).accepted).toHaveLength(1);
});

it("rejects omitted admission, changed intent and changed authority without reserving a run", async () => {
  const target = await setup("conflict");
  await expect(target.submit()).rejects.toThrow();
  await target.admit();
  for (const replay of [
    { ...target.replay, authorityId: "other" },
    { ...target.replay, intent: { action: "run", jobId: "other", mode: "force" } as const },
    { ...target.replay, ref: { ...target.replay.ref, effectId: "invented" } },
  ]) {
    await expect(target.submit(replay)).rejects.toThrow(/replay/);
  }
  expect(await target.inspect()).toEqual({ state: "prepared" });
});

it("fences late dispatch and permits only a reviewed bounded next attempt", async () => {
  const target = await setup("fenced");
  await target.admit();
  const second = { ...target.replay, ref: { ...target.replay.ref, attempt: 2 } };
  await expect(
    execute("cron.replayRetry", {
      storeKey: target.storeKey,
      replay: second,
      evidence: "timeout",
      nowMs: now,
    }),
  ).rejects.toThrow(/non-execution/);
  expect(
    await execute("cron.replayClose", { storeKey: target.storeKey, replay: target.replay }),
  ).toEqual({ state: "not-applied" });
  expect((await target.submit()).replayed).toEqual([{ state: "not-applied" }]);
  await expect(target.submit(second)).rejects.toThrow(/not authorized/);
  await execute("cron.replayRetry", {
    storeKey: target.storeKey,
    replay: second,
    evidence: "reviewed target fence",
    nowMs: now + 1,
  });
  expect((await target.submit(second)).accepted).toHaveLength(1);
  await expect(
    execute("cron.replayRetry", {
      storeKey: target.storeKey,
      replay: { ...second, ref: { ...second.ref, attempt: 3 } },
      evidence: "again",
      nowMs: now + 2,
    }),
  ).rejects.toThrow(/budget/);
});

it("recovers accepted replay facts after the real worker commits and its response is lost", async () => {
  const target = await setup("lost");
  await target.admit();
  const fault = loseFirstCronMutationReply("cron.requestRuns");
  try {
    await expect(target.submit()).rejects.toThrow();
    await fault.waitForExit();
    expect(fault.wasDropped()).toBe(true);
  } finally {
    await fault.close();
  }
  context = captureOpenClawStateWorkerContext();
  expect(await target.inspect()).toMatchObject({ state: "accepted" });
  const retried = await target.submit();
  expect(retried.accepted).toEqual([]);
  expect(retried.replayed).toEqual([await target.inspect()]);
});

it("rolls back both enqueue and replay acceptance when current authority is revoked before commit", async () => {
  const target = await setup("revoked");
  await target.admit();
  await expect(
    target.submit(target.replay, () => {
      throw new Error("synthetic authority revoked");
    }),
  ).rejects.toThrow();
  expect(await target.inspect()).toEqual({ state: "prepared" });
  const retry = await target.submit();
  expect(retry.accepted).toHaveLength(1);
  expect(retry.replayed).toBeUndefined();
});

it("serializes close against dispatch so a not-applied receipt cannot conceal an accepted run", async () => {
  const target = await setup("close-race");
  await target.admit();
  const [submitted, closed] = await Promise.all([
    target.submit(),
    execute("cron.replayClose", { storeKey: target.storeKey, replay: target.replay }),
  ]);
  expect(await target.inspect()).toEqual(closed);
  expect(submitted.accepted.length).toBe(closed.state === "accepted" ? 1 : 0);
  expect((await target.submit()).accepted).toEqual([]);
});

it("retains acceptance after execution receipts are pruned and the database reopens", async () => {
  const target = await setup("retained");
  await target.admit();
  await target.submit();
  const expected = await target.inspect();
  await execute("cron.cancelRequests", {
    storeKey: target.storeKey,
    receiptIds: [cronReplayRunId(target.storeKey, target.replay.ref)],
    nowMs: now + 1,
    reason: "fixture completion",
  });
  await closeOpenClawStateDatabaseAsync();
  const db = openOpenClawStateDatabase().db;
  // Offline fixture simulates bounded receipt pruning; replay authority is separate.
  db.prepare("DELETE FROM cron_run_receipts WHERE store_key = ?").run(target.storeKey);
  await closeOpenClawStateDatabaseAsync();
  context = captureOpenClawStateWorkerContext();
  expect(await target.inspect()).toEqual(expected);
  expect((await target.submit()).replayed).toEqual([expected]);
});

it("persists the exact add result atomically and replays it after the job is removed", async () => {
  const target = await setup("add");
  const input = {
    ...target.job,
    id: "added",
    name: "added",
    declarationKey: "synthetic-declaration",
  };
  const replay: CronReplayRequest = {
    ...target.replay,
    ref: { ...target.replay.ref, effectId: "add" },
    intent: { action: "add", intentHash: cronReplayIntentHash(input) },
  };
  await target.admit(replay);
  const mutation = {
    storeKey: target.storeKey,
    replay,
    jobMutation: { kind: "add" as const, input, options: { captured: false } },
    snapshot: { nowMs: now },
  };
  const first = await execute("cron.mutateJobs", mutation);
  expect(first).toHaveProperty("outcome.replayOutcome.state", "accepted");
  if (!("outcome" in first)) {
    throw new Error("fixture add refused");
  }
  expect(first.outcome.replayOutcome).toMatchObject({
    state: "accepted",
    result: { id: "added", created: true },
  });
  await execute("cron.mutateJobs", {
    storeKey: target.storeKey,
    jobMutation: { kind: "remove", ids: ["added"] },
    snapshot: { nowMs: now + 1 },
  });
  const duplicate = await execute("cron.mutateJobs", mutation);
  expect(duplicate).toMatchObject({
    outcome: { changed: false, replayed: true, replayOutcome: first.outcome.replayOutcome },
  });
  if (!("outcome" in duplicate)) {
    throw new Error("fixture duplicate refused");
  }
  expect(duplicate.outcome.store.jobs.map((job) => job.id)).toEqual(["add"]);
});
