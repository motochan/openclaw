import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createCronRegressionState } from "../../test/helpers/cron/service-regression-fixtures.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../test-utils/openclaw-test-state.js";
import { cronReplayIntentHash, type CronReplayRequest } from "./replay.js";
import { CronService } from "./service.js";
import { saveCronStore } from "./store.js";
import type { CronJob, CronJobCreate } from "./types.js";

const now = 1_800_000_000_000;
let fixture: OpenClawTestState;
const services: CronService[] = [];

beforeAll(async () => {
  fixture = await createOpenClawTestState({ label: "cron-replay-service" });
});
afterAll(async () => {
  for (const cron of services) {
    cron.stop();
  }
  await Promise.all(services.map((cron) => cron.waitForIdle()));
  await fixture?.cleanup();
});

function service(
  storePath: string,
  runCommandJob = vi.fn(async () => ({ status: "ok" as const })),
) {
  const state = createCronRegressionState({
    storePath,
    nowMs: () => now,
    defaultAgentId: "main",
    isAgentAvailable: () => true,
    runCommandJob,
    runIsolatedAgentJob: async () => ({ status: "ok" }),
    listConfiguredChannels: () => [],
  });
  const cron = new CronService(state.deps);
  services.push(cron);
  return cron;
}

async function runFixture(label: string) {
  const storePath = fixture.statePath("cron", `${label}.json`);
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
    payload: { kind: "command", argv: ["synthetic", label] },
    delivery: { mode: "none" },
    state: { nextRunAtMs: now + 60_000 },
  };
  await saveCronStore(storePath, { version: 1, jobs: [job] });
  const runner = vi.fn(async () => ({ status: "ok" as const }));
  const cron = service(storePath, runner);
  const replay: CronReplayRequest = {
    ref: { operationId: label, effectId: "run", attempt: 1 },
    authorityId: "operator",
    intent: { action: "run", jobId: job.id, mode: "force" },
  };
  await cron.replay(
    { action: "admit", replay, maxAttempts: 2, evidence: "reviewed fixture" },
    () => {},
  );
  return { cron, replay, job, runner, storePath };
}

it("replays the original run acknowledgement after caller response loss and service replacement", async () => {
  const { cron, replay, job, runner, storePath } = await runFixture("lost-caller-reply");
  const accepted = await cron.enqueueRun(job.id, "force", { replay });
  if (!("enqueued" in accepted)) {
    throw new Error("expected accepted run");
  }
  await cron.waitForManualRun(accepted.runId, 5_000);
  expect(runner).toHaveBeenCalledTimes(1);
  // The caller loses its response after the real native enqueue and execution.
  const replacement = service(storePath, runner);
  expect(await replacement.enqueueRun(job.id, "force", { replay })).toEqual(accepted);
  expect(await cron.enqueueRun(job.id, "force", { replay })).toEqual(accepted);
  expect(runner).toHaveBeenCalledTimes(1);
});

it("keeps closed attempts held until reviewed retry and rejects changed run intent", async () => {
  const { cron, replay, job, runner } = await runFixture("reviewed-retry");
  expect(await cron.replay({ action: "close", replay }, () => {})).toEqual({
    state: "not-applied",
  });
  await expect(cron.enqueueRun(job.id, "force", { replay })).rejects.toThrow("closed");
  await expect(cron.enqueueRun(job.id, "due", { replay })).rejects.toThrow("intent");
  const next = { ...replay, ref: { ...replay.ref, attempt: 2 } };
  await expect(cron.enqueueRun(job.id, "force", { replay: next })).rejects.toThrow();
  expect(runner).not.toHaveBeenCalled();
  await cron.replay(
    { action: "retry", replay: next, evidence: "reviewed non-execution" },
    () => {},
  );
  const accepted = await cron.enqueueRun(job.id, "force", { replay: next });
  if (!("enqueued" in accepted)) {
    throw new Error("expected accepted retry");
  }
  await cron.waitForManualRun(accepted.runId, 5_000);
  expect(runner).toHaveBeenCalledTimes(1);
});

it("returns the original declarative add receipt after an edit and service replacement", async () => {
  const storePath = fixture.statePath("cron", "add-receipt.json");
  const cron = service(storePath);
  const input: CronJobCreate = {
    name: "Original",
    declarationKey: "fixture-declaration",
    agentId: "main",
    enabled: false,
    schedule: { kind: "every", everyMs: 60_000 },
    sessionTarget: "isolated",
    wakeMode: "next-heartbeat",
    payload: { kind: "command", argv: ["synthetic"] },
    delivery: { mode: "none" },
  };
  const replay: CronReplayRequest = {
    ref: { operationId: "declare", effectId: "job", attempt: 1 },
    authorityId: "operator",
    intent: { action: "add", intentHash: cronReplayIntentHash({ input, enabledExplicit: true }) },
  };
  await cron.replay(
    { action: "admit", replay, maxAttempts: 1, evidence: "reviewed declaration" },
    () => {},
  );
  const accepted = await cron.add(input, { replay, enabledExplicit: true });
  await cron.update(accepted.id, { name: "Later independent edit" });
  const replacement = service(storePath);
  expect(await replacement.add(input, { replay, enabledExplicit: true })).toEqual(accepted);
  expect((await replacement.readJob(accepted.id))?.name).toBe("Later independent edit");
  expect(await replacement.list({ includeDisabled: true })).toHaveLength(1);
  await expect(
    replacement.add({ ...input, name: "Changed intent" }, { replay, enabledExplicit: true }),
  ).rejects.toThrow("intent");
});
