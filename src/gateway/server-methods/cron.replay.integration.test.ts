import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createCronRegressionState } from "../../../test/helpers/cron/service-regression-fixtures.js";
import { CronService } from "../../cron/service.js";
import { saveCronStore } from "../../cron/store.js";
import type { CronJob } from "../../cron/types.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../../test-utils/openclaw-test-state.js";
import { createDirectChatContext } from "../server-chat.agent-events.test-helpers.js";
import { createSyntheticPluginRuntimeClient } from "../server-plugin-runtime-client.js";
import { cronHandlers } from "./cron.js";
import { createCronCallerClient } from "./cron.validation.test-support.js";
import type { GatewayClient } from "./types.js";

let fixture: OpenClawTestState;
const services: CronService[] = [];
const now = 1_800_000_000_000;
beforeAll(async () => {
  fixture = await createOpenClawTestState({ label: "cron-replay-gateway" });
});
afterAll(async () => {
  for (const cron of services) {
    cron.stop();
  }
  await Promise.all(services.map((cron) => cron.waitForIdle()));
  await fixture?.cleanup();
});

async function setup(label: string) {
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
    payload: { kind: "agentTurn", message: "synthetic fixture", toolsAllow: [] },
    delivery: { mode: "none" },
    state: { nextRunAtMs: now + 60_000 },
  };
  await saveCronStore(storePath, { version: 1, jobs: [job] });
  const runner = vi.fn(async () => ({ status: "ok" as const }));
  const cron = new CronService(
    createCronRegressionState({
      storePath,
      nowMs: () => now,
      defaultAgentId: "main",
      isAgentAvailable: () => true,
      runIsolatedAgentJob: runner,
      listConfiguredChannels: () => [],
    }).deps,
  );
  services.push(cron);
  const context = createDirectChatContext({
    cron,
    cronStorePath: storePath,
    getRuntimeConfig: () => ({}),
    validateAgentRuntimeApprovalAuthority: undefined,
  });
  const operator = createSyntheticPluginRuntimeClient({ scopes: ["operator.admin"] });
  async function invoke(
    method: string,
    params: Record<string, unknown>,
    client: GatewayClient = operator,
  ) {
    const respond = vi.fn();
    const handler = cronHandlers[method];
    if (!handler) {
      throw new Error(`missing handler ${method}`);
    }
    await handler({
      req: { type: "req", id: `${label}-${method}`, method, params },
      params,
      respond,
      context,
      client,
      isWebchatConnect: () => false,
      hasCurrentClientAuthority: () => true,
    });
    return respond.mock.calls[0];
  }
  const reference = { operationId: label, effectId: "run", attempt: 1 };
  const intent = { action: "run", jobId: job.id, mode: "force" };
  const admission = {
    operationId: label,
    effectId: "run",
    authorityId: "operator",
    intent,
    evidence: "reviewed operator fixture",
    maxAttempts: 2,
  };
  return { cron, runner, invoke, reference, admission, job };
}

it("uses the real Gateway run boundary and native retained receipt on a caller retry", async () => {
  const { cron, runner, invoke, reference, admission, job } = await setup("gateway-replay");
  expect(await invoke("cron.replay.admit", admission)).toEqual([
    true,
    { state: "prepared" },
    undefined,
  ]);
  const first = await invoke("cron.run", { id: job.id, mode: "force", replay: reference });
  expect(first?.[0]).toBe(true);
  const result = first?.[1] as { enqueued: boolean; runId: string };
  expect(result.enqueued).toBe(true);
  await cron.waitForManualRun(result.runId, 5_000);
  expect(await invoke("cron.run", { id: job.id, mode: "force", replay: reference })).toEqual(first);
  expect(await cron.remove(job.id)).toMatchObject({ removed: true });
  expect(await invoke("cron.run", { id: job.id, mode: "force", replay: reference })).toEqual(first);
  expect(runner).toHaveBeenCalledTimes(1);
});

it("denies model-created admissions and omitted identities before any scheduler effect", async () => {
  const { runner, invoke, admission, job } = await setup("gateway-agent-denial");
  const agent = createCronCallerClient("main");
  agent.connect.scopes = ["operator.admin"];
  const deniedAdmission = await invoke("cron.replay.admit", admission, agent);
  expect(deniedAdmission?.[0]).toBe(false);
  expect(deniedAdmission?.[2]?.message).toContain("trusted operator.admin");
  const omitted = await invoke("cron.run", { id: job.id, mode: "force" }, agent);
  expect(omitted?.[0]).toBe(false);
  expect(omitted?.[2]?.message).toContain("host-admitted replay identity");
  const missing = await invoke("cron.replay.inspect", {
    operationId: admission.operationId,
    effectId: admission.effectId,
    authorityId: admission.authorityId,
    intent: admission.intent,
    attempt: 1,
  });
  expect(missing?.[0]).toBe(false);
  expect(missing?.[2]?.message).toContain("admission missing");
  expect(runner).not.toHaveBeenCalled();
});

it("prepares the normalized add intent before admission and reuses its native job receipt", async () => {
  const { cron, invoke } = await setup("gateway-add-replay");
  const input = {
    name: "Gateway declaration",
    declarationKey: "fixture-gateway-declaration",
    agentId: "main",
    enabled: false,
    schedule: { kind: "at", at: new Date(now + 60_000).toISOString() },
    sessionTarget: "isolated",
    wakeMode: "next-heartbeat",
    payload: { kind: "command", argv: ["synthetic"] },
    delivery: { mode: "none" },
  };
  const prepared = await invoke("cron.replay.prepareAdd", input);
  expect(prepared?.[0]).toBe(true);
  expect(prepared?.[1]).toMatchObject({ authorityId: "operator", intent: { action: "add" } });
  const replay = { operationId: "gateway-add-operation", effectId: "declaration", attempt: 1 };
  const admitted = await invoke("cron.replay.admit", {
    operationId: replay.operationId,
    effectId: replay.effectId,
    authorityId: prepared?.[1].authorityId,
    intent: prepared?.[1].intent,
    evidence: "reviewed prepared declaration",
    maxAttempts: 1,
  });
  expect(admitted?.[0]).toBe(true);
  const first = await invoke("cron.add", { ...input, replay });
  expect(first?.[0]).toBe(true);
  expect(first?.[1]).toMatchObject({ created: true, job: { name: "Gateway declaration" } });
  const clock = vi.spyOn(Date, "now").mockReturnValue(now + 180_000);
  try {
    expect(await invoke("cron.add", { ...input, replay })).toEqual(first);
  } finally {
    clock.mockRestore();
  }
  expect(
    (await cron.list({ includeDisabled: true })).filter(
      (job) => job.declarationKey === input.declarationKey,
    ),
  ).toHaveLength(1);
});
