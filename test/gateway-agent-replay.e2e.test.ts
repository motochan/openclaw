import { randomUUID } from "node:crypto";
import type { ServerResponse } from "node:http";
import { expect, it } from "vitest";
import { connectGatewayClient, disconnectGatewayClient } from "../src/gateway/test-helpers.e2e.js";
import { isRecord } from "../src/utils.js";
import {
  createOpenAiResponsesModelConfig,
  startOpenAiResponsesModelServer,
} from "./helpers/openai-responses-model-fixture.js";
import {
  writeOpenAiResponsesSse,
  writeOpenAiResponsesText,
} from "./helpers/openai-responses-sse.js";
import { createOpenClawTestInstance } from "./helpers/openclaw-test-instance.js";
import { createDeferred } from "./helpers/promise.js";

const SESSION = "agent:main:replay-turn-proof";
const COMPLETE = "REPLAY_TURN_QUALIFIED";

type Prepared = {
  authorityId: string;
  intent: { action: "add"; intentHash: string };
  executionParams: Record<string, unknown>;
};

function readToolOutput(body: Record<string, unknown>, sequence: number): string {
  if (!Array.isArray(body.input)) {
    throw new Error("missing model input");
  }
  const output = body.input.findLast(
    (item) =>
      isRecord(item) &&
      item.type === "function_call_output" &&
      item.call_id === `call_replay_${sequence}`,
  );
  if (!isRecord(output) || typeof output.output !== "string") {
    throw new Error("missing tool output");
  }
  return output.output;
}

function readPrepared(body: Record<string, unknown>, sequence: number): Prepared {
  const result: unknown = JSON.parse(readToolOutput(body, sequence));
  if (
    !isRecord(result) ||
    typeof result.authorityId !== "string" ||
    !isRecord(result.intent) ||
    result.intent.action !== "add" ||
    typeof result.intent.intentHash !== "string" ||
    !isRecord(result.executionParams)
  ) {
    throw new Error("invalid preparation result");
  }
  return {
    authorityId: result.authorityId,
    intent: { action: "add", intentHash: result.intent.intentHash },
    executionParams: result.executionParams,
  };
}

function toolResponse(response: ServerResponse, sequence: number, args: Record<string, unknown>) {
  const item = {
    type: "function_call",
    id: `fc_replay_${sequence}`,
    call_id: `call_replay_${sequence}`,
    name: "automations",
    arguments: JSON.stringify(args),
    status: "completed",
  };
  writeOpenAiResponsesSse(response, [
    {
      type: "response.output_item.added",
      output_index: 0,
      item: { ...item, status: "in_progress", arguments: "" },
    },
    {
      type: "response.function_call_arguments.done",
      item_id: item.id,
      output_index: 0,
      arguments: item.arguments,
    },
    { type: "response.output_item.done", output_index: 0, item },
    {
      type: "response.completed",
      response: {
        id: `resp_replay_${sequence}`,
        status: "completed",
        output: [item],
        usage: { input_tokens: 10, output_tokens: 2, total_tokens: 12 },
      },
    },
  ]);
}

it(
  "qualifies replay admission through full agent turns and cancellation before mutation",
  { timeout: 180_000 },
  async () => {
    const reviewed = createDeferred<Prepared>();
    const admitted = createDeferred();
    const cancelledPrepared = createDeferred<Prepared>();
    const providerClosed = createDeferred();
    const modelFailure = createDeferred<never>();
    // Always observe model-side failures, including ones raised after the main wait.
    void modelFailure.promise.catch(() => undefined);
    const requests: Record<string, unknown>[] = [];
    const replay = { operationId: "full-agent-turn", effectId: "add", attempt: 1 };
    const cancelledReplay = { ...replay, effectId: "cancelled-add" };
    let phase: "normal" | "cancel" = "normal";
    let normalStep = 0;
    let cancelStep = 0;
    let prepared: Prepared;
    let first: unknown;
    const rawJob = {
      name: "Full agent replay",
      enabled: false,
      schedule: { kind: "every", everyMs: 86_400_000 },
      sessionTarget: "current",
      payload: { kind: "agentTurn", message: "Synthetic scheduled work" },
      delivery: { mode: "none" },
    };
    const model = await startOpenAiResponsesModelServer({
      modelId: "replay-proof",
      bindError: "replay model fixture did not bind",
      onRequest: async (request, response) => {
        try {
          const chunks: Buffer[] = [];
          for await (const chunk of request) {
            chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
          }
          const parsed: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          if (!isRecord(parsed)) {
            throw new Error("invalid model request");
          }
          requests.push(parsed);
          if (phase === "cancel") {
            if (cancelStep++ === 0) {
              toolResponse(response, 100, {
                action: "prepare_add",
                job: { ...rawJob, name: "Cancelled agent replay" },
              });
            } else {
              // Hold the proposed add at the provider boundary until the actual runner retires.
              response.once("close", () => {
                if (!response.writableFinished) {
                  providerClosed.resolve();
                } else {
                  modelFailure.reject(new Error("held provider response unexpectedly completed"));
                }
              });
              cancelledPrepared.resolve(readPrepared(parsed, 100));
            }
            return;
          }
          switch (normalStep++) {
            case 0:
              expect(JSON.stringify(parsed.tools)).toContain('"automations"');
              toolResponse(response, 0, { action: "prepare_add", job: rawJob });
              break;
            case 1:
              prepared = readPrepared(parsed, 0);
              toolResponse(response, 1, { action: "add", job: prepared.executionParams });
              break;
            case 2:
              expect(readToolOutput(parsed, 1)).toContain("host-admitted replay identity");
              toolResponse(response, 2, { action: "add", job: prepared.executionParams, replay });
              break;
            case 3:
              expect(readToolOutput(parsed, 2)).toContain("admission missing");
              reviewed.resolve(prepared);
              await admitted.promise;
              toolResponse(response, 3, { action: "add", job: prepared.executionParams, replay });
              break;
            case 4:
              first = JSON.parse(readToolOutput(parsed, 3));
              expect(first).toMatchObject({
                id: expect.any(String),
                name: rawJob.name,
                owner: { agentId: "main", sessionKey: SESSION },
              });
              toolResponse(response, 4, { action: "add", job: prepared.executionParams, replay });
              break;
            case 5:
              expect(JSON.parse(readToolOutput(parsed, 4))).toEqual(first);
              toolResponse(response, 5, {
                action: "add",
                job: { ...prepared.executionParams, name: "Changed intent" },
                replay,
              });
              break;
            case 6:
              expect(readToolOutput(parsed, 5)).toContain("intent/authority conflicts");
              writeOpenAiResponsesText(response, {
                text: COMPLETE,
                messageId: "msg_replay_done",
                responseId: "resp_replay_done",
              });
              break;
            default:
              throw new Error("unexpected extra model request");
          }
        } catch (error) {
          modelFailure.reject(error);
          throw error;
        }
      },
    });
    let instance: Awaited<ReturnType<typeof createOpenClawTestInstance>> | undefined;
    let client: Awaited<ReturnType<typeof connectGatewayClient>> | undefined;
    let pending: Promise<unknown> | undefined;
    try {
      const config = createOpenAiResponsesModelConfig({
        baseUrl: model.baseUrl,
        providerId: "replay-proof",
        modelId: "replay-proof",
        reasoning: false,
        toolProfile: "coding",
      });
      config.tools = {
        profile: "coding",
        codeMode: false,
        toolSearch: false,
        allow: ["automations"],
      };
      config.cron = { enabled: false };
      instance = await createOpenClawTestInstance({
        name: "agent-replay-proof",
        config,
        env: { OPENCLAW_SKIP_PROVIDERS: undefined, OPENCLAW_TEST_MINIMAL_GATEWAY: undefined },
      });
      await instance.startGateway();
      client = await connectGatewayClient({
        url: instance.url,
        token: instance.gatewayToken,
        scopes: ["operator.admin", "operator.read", "operator.write"],
      });
      const runId = randomUUID();
      pending = client.request(
        "agent",
        {
          sessionKey: SESSION,
          deliver: false,
          message:
            "Prepare the requested automation and wait for the host to admit it; preserve its replay identity.",
          idempotencyKey: runId,
        },
        { expectFinal: true, timeoutMs: 120_000 },
      );
      void pending.catch(() => undefined);
      const proposal = await Promise.race([
        reviewed.promise,
        modelFailure.promise,
        pending.then(() => {
          throw new Error("turn ended before host review");
        }),
      ]);
      expect(proposal.authorityId).toBe(`agent:main:${SESSION}`);
      expect(proposal.executionParams).toMatchObject(rawJob);
      expect(await client.request("cron.list", { includeDisabled: true })).toMatchObject({
        jobs: [
          { declarationKey: "heartbeat:main", enabled: false, payload: { kind: "heartbeat" } },
        ],
      });
      const hostEvidence = "private-host-review-full-turn";
      expect(
        await client.request("cron.replay.admit", {
          operationId: replay.operationId,
          effectId: replay.effectId,
          authorityId: proposal.authorityId,
          intent: proposal.intent,
          maxAttempts: 1,
          evidence: hostEvidence,
        }),
      ).toEqual({ state: "prepared" });
      admitted.resolve();
      const result = await Promise.race([pending, modelFailure.promise]);
      expect(result).toMatchObject({
        runId,
        status: "ok",
        result: { payloads: expect.arrayContaining([expect.objectContaining({ text: COMPLETE })]) },
      });
      expect(normalStep).toBe(7);
      expect(await client.request("cron.list", { includeDisabled: true })).toMatchObject({
        jobs: expect.arrayContaining([expect.objectContaining({ name: rawJob.name })]),
      });
      const acceptedJobs = await client.request<{ jobs: unknown[] }>("cron.list", {
        includeDisabled: true,
      });
      expect(acceptedJobs.jobs).toHaveLength(2);
      expect(
        await client.request("cron.replay.inspect", {
          ...replay,
          authorityId: proposal.authorityId,
          intent: proposal.intent,
        }),
      ).toMatchObject({ state: "accepted", result: first });

      phase = "cancel";
      const cancelledRunId = randomUUID();
      pending = client.request(
        "agent",
        {
          sessionKey: SESSION,
          deliver: false,
          message: "Prepare the second automation, then wait for host admission.",
          idempotencyKey: cancelledRunId,
        },
        { expectFinal: true, timeoutMs: 120_000 },
      );
      void pending.catch(() => undefined);
      const cancelled = await Promise.race([
        cancelledPrepared.promise,
        modelFailure.promise,
        pending.then(() => {
          throw new Error("turn ended before cancellation boundary");
        }),
      ]);
      expect(cancelled.authorityId).toBe(`agent:main:${SESSION}`);
      expect(cancelled.executionParams).toMatchObject({
        ...rawJob,
        name: "Cancelled agent replay",
      });
      expect(
        await client.request("cron.replay.admit", {
          operationId: cancelledReplay.operationId,
          effectId: cancelledReplay.effectId,
          authorityId: cancelled.authorityId,
          intent: cancelled.intent,
          maxAttempts: 1,
          evidence: hostEvidence,
        }),
      ).toEqual({ state: "prepared" });
      expect(
        await client.request("chat.abort", { sessionKey: SESSION, runId: cancelledRunId }),
      ).toMatchObject({ aborted: true, runIds: [cancelledRunId] });
      expect(await pending).toMatchObject({
        runId: cancelledRunId,
        status: "timeout",
        summary: "aborted",
        stopReason: "rpc",
      });
      await Promise.race([providerClosed.promise, modelFailure.promise]);
      expect(
        await client.request("agent.wait", { runId: cancelledRunId, timeoutMs: 1000 }),
      ).toMatchObject({ status: "error", stopReason: "rpc" });
      expect(
        await client.request("cron.replay.inspect", {
          ...cancelledReplay,
          authorityId: cancelled.authorityId,
          intent: cancelled.intent,
        }),
      ).toEqual({ state: "prepared" });
      expect(await client.request("cron.list", { includeDisabled: true })).toMatchObject({
        jobs: expect.arrayContaining([expect.objectContaining({ name: rawJob.name })]),
      });
      const afterCancellation = await client.request<{ jobs: unknown[] }>("cron.list", {
        includeDisabled: true,
      });
      expect(afterCancellation.jobs).toEqual(acceptedJobs.jobs);
      expect(cancelStep).toBe(2);
      const serializedRequests = JSON.stringify(requests);
      expect(serializedRequests).not.toContain(instance.gatewayToken);
      expect(serializedRequests).not.toContain(hostEvidence);
    } finally {
      admitted.resolve();
      try {
        if (client) {
          await disconnectGatewayClient(client);
        }
      } finally {
        try {
          await instance?.cleanup();
        } finally {
          try {
            await model.close();
          } finally {
            await Promise.allSettled(pending ? [pending] : []);
          }
        }
      }
    }
  },
);
