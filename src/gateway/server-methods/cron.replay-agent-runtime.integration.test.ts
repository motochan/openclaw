import fs from "node:fs/promises";
import path from "node:path";
import { expectDefined } from "@openclaw/normalization-core";
import { beforeEach, afterEach, expect, it, vi } from "vitest";
import { persistAuthProfileBatch } from "../../agents/auth-profiles/upsert-with-lock.js";
import { withPreparedEmbeddedGatewayTools } from "../../agents/embedded-agent-runner/run/attempt-gateway-tools.js";
import { AUTOMATIONS_TOOL_NAME } from "../../agents/tools/automations-tool-name.js";
import { callGatewayTool } from "../../agents/tools/gateway.js";
import { setRuntimeConfigSnapshot } from "../../config/config.js";
import {
  bindGatewayContextResolver,
  clearGatewayContextResolver,
} from "../../plugins/runtime/gateway-request-scope.js";
import { isRecord } from "../../utils.js";
import { revokeMcpLoopbackClientGrant } from "../mcp-grant-store.js";
import { closeMcpLoopbackServer, ensureMcpLoopbackServer } from "../mcp-http.js";
import { getActiveMcpLoopbackRuntime } from "../mcp-http.loopback-runtime.js";
import { createSyntheticPluginRuntimeClient } from "../server-plugin-runtime-client.js";
import { cronHandlers } from "./cron.js";
import { startHostAdmissionService } from "./host-admission-service.test-support.js";
import {
  SESSION,
  SESSION_ID,
  stateDir,
  cfg,
  inRun,
  createCronFixture,
  createCreatorTransportTools,
  installRequesterCronAuthorityTestHooks,
} from "./requester-cron-authority.test-support.js";

installRequesterCronAuthorityTestHooks();
beforeEach(async () => {
  vi.stubEnv("OPENAI_API_KEY", undefined);
  vi.stubEnv("OPENAI_OAUTH_TOKEN", undefined);
  await persistAuthProfileBatch({
    stateDir,
    profiles: [
      {
        profileId: "openai:replay-test",
        credential: { type: "api_key", provider: "openai", key: "synthetic-replay-test-key" },
      },
    ],
  });
});
afterEach(async () => {
  await closeMcpLoopbackServer();
});
// Attached-node discovery is unrelated to the admitted automation transport.
vi.mock("../../agents/node-exec-availability.js", () => ({
  loadNodeExecAvailability: async () => ({ cacheKey: "no-nodes", isAvailable: () => false }),
}));

function details(result: unknown): Record<string, unknown> {
  if (isRecord(result) && isRecord(result.details)) {
    return result.details;
  }
  if (isRecord(result) && Array.isArray(result.content)) {
    const text = result.content.find((item) => isRecord(item) && item.type === "text");
    if (isRecord(text) && typeof text.text === "string") {
      const parsed: unknown = JSON.parse(text.text);
      if (isRecord(parsed)) {
        return parsed;
      }
    }
  }
  throw new Error("expected native or MCP tool result");
}

it.each(["embedded", "cli"] as const)(
  "binds %s prepared adds to the admitted agent and keeps replay control host-only",
  async (transport) => {
    const config = {
      ...cfg,
      agents: { ...cfg.agents, defaults: { model: "openai/gpt-4.1" } },
      tools: { allow: [AUTOMATIONS_TOOL_NAME] },
    };
    setRuntimeConfigSnapshot(config);
    const fixture = createCronFixture(undefined, config);
    const replay = { operationId: "runtime-qualified-add", effectId: "add", attempt: 1 };
    let retained: (() => Promise<unknown>) | undefined;
    let mcpCapture: { token: string; runtimeOwnerToken: string; captureKey: string } | undefined;
    if (transport === "cli") {
      await ensureMcpLoopbackServer(0);
    }
    await inRun("runtime-qualified-run", undefined, async (_identity, admitted) => {
      bindGatewayContextResolver(admitted, () => fixture.context);
      try {
        const tools = await createCreatorTransportTools({
          transport,
          config,
          admitted,
          senderIsOwner: true,
        });
        mcpCapture = tools.mcpCapture;
        const invoke = (args: Record<string, unknown>) => tools.invoke(AUTOMATIONS_TOOL_NAME, args);
        const prepared = details(
          await invoke({
            action: "prepare_add",
            job: {
              name: "Admitted runtime add",
              enabled: false,
              schedule: { kind: "every", everyMs: 60_000 },
              sessionTarget: "current",
              payload: { kind: "agentTurn", message: "Synthetic qualification" },
              delivery: { mode: "none" },
            },
          }),
        );
        expect(prepared.authorityId).toBe(`agent:main:${SESSION}`);
        expect(await fixture.read()).toEqual([]);
        const job = prepared.executionParams;
        expect(isRecord(job)).toBe(true);
        const admission = {
          ...replay,
          authorityId: prepared.authorityId,
          intent: prepared.intent,
          evidence: "host-reviewed runtime fixture",
          maxAttempts: 1,
        };
        const { attempt: _attempt, ...admitParams } = admission;
        const runtimeCall = (method: string, params: Record<string, unknown>) =>
          withPreparedEmbeddedGatewayTools(
            {
              admittedRunContext: admitted,
              agentId: "main",
              sessionKey: SESSION,
              sessionId: SESSION_ID,
              agentHarnessId: "openclaw",
            },
            () => callGatewayTool(method, {}, params),
          );
        await expect(runtimeCall("cron.replay.admit", admitParams)).rejects.toThrow(
          /trusted operator.admin/,
        );
        await expect(invoke({ action: "add", job })).rejects.toThrow(
          /host-admitted replay identity/,
        );
        await expect(invoke({ action: "add", job, replay })).rejects.toThrow(/admission missing/);
        const respond = vi.fn();
        await expectDefined(
          cronHandlers["cron.replay.admit"],
          "host admission",
        )({
          req: { type: "req", id: "host-admit", method: "cron.replay.admit", params: admitParams },
          params: admitParams,
          respond,
          context: fixture.context,
          client: createSyntheticPluginRuntimeClient({ scopes: ["operator.admin"] }),
          isWebchatConnect: () => false,
          hasCurrentClientAuthority: () => true,
        });
        expect(respond).toHaveBeenCalledWith(true, { state: "prepared" }, undefined);
        const controlRef = {
          ...replay,
          authorityId: prepared.authorityId,
          intent: prepared.intent,
        };
        for (const method of ["cron.replay.inspect", "cron.replay.close", "cron.replay.retry"]) {
          await expect(
            runtimeCall(method, {
              ...controlRef,
              ...(method.endsWith("retry") ? { evidence: "model cannot authorize retry" } : {}),
            }),
          ).rejects.toThrow(/trusted operator.admin/);
        }
        const request = { action: "add", job, replay };
        const first = details(await invoke(request));
        expect(details(await invoke(request))).toEqual(first);
        expect(await fixture.read()).toMatchObject([{ name: "Admitted runtime add" }]);
        expect(await fixture.read()).toHaveLength(1);
        await expect(invoke({ ...request, gatewayToken: "synthetic-override" })).rejects.toThrow(
          /trusted local gateway context/,
        );
        retained = () => invoke(request);
      } finally {
        clearGatewayContextResolver(admitted);
      }
    });
    if (mcpCapture) {
      try {
        const runtime = expectDefined(getActiveMcpLoopbackRuntime(), "MCP runtime");
        const response = await fetch(`http://127.0.0.1:${runtime.port}/mcp`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${mcpCapture.token}`,
            "content-type": "application/json",
            "x-openclaw-cli-capture-key": mcpCapture.captureKey,
          },
          body: JSON.stringify({ jsonrpc: "2.0", id: 99, method: "tools/list" }),
        });
        expect(response.status).toBe(401);
      } finally {
        revokeMcpLoopbackClientGrant(mcpCapture.token);
      }
    } else {
      await expect(expectDefined(retained, "retained tool")()).rejects.toThrow(
        /authority|active|admitt|available/i,
      );
    }
    expect(await fixture.read()).toHaveLength(1);
  },
);

it.runIf(process.env.JAMIE_HARNESS_ROOT !== undefined)(
  "recovers the real host journal across service replacement using live agent preparation",
  async () => {
    const harnessRoot = expectDefined(process.env.JAMIE_HARNESS_ROOT, "host adapter checkout");
    const config = {
      ...cfg,
      agents: { ...cfg.agents, defaults: { model: "openai/gpt-4.1" } },
      tools: { allow: [AUTOMATIONS_TOOL_NAME] },
    };
    setRuntimeConfigSnapshot(config);
    const fixture = createCronFixture(undefined, config);
    const privateDir = path.join(stateDir, "host-admission");
    await fs.mkdir(privateDir, { mode: 0o700 });
    const database = path.join(privateDir, "journal.sqlite");
    const reference = { operation: "journal-runtime-operation", effect: "add" };
    const rawJob = {
      name: "Journal-backed runtime add",
      enabled: false,
      schedule: { kind: "every", everyMs: 60_000 },
      sessionTarget: "current",
      payload: { kind: "agentTurn", message: "Synthetic host journal qualification" },
      delivery: { mode: "none" },
    };
    let host: Awaited<ReturnType<typeof startHostAdmissionService>> | undefined;
    let loseAdmissionReply = true;
    let loseExecutionReply = true;
    let prepareCalls = 0;
    try {
      await inRun("journal-runtime-run", undefined, async (_identity, admitted) => {
        bindGatewayContextResolver(admitted, () => fixture.context);
        try {
          const tools = await createCreatorTransportTools({
            transport: "embedded",
            config,
            admitted,
            senderIsOwner: true,
          });
          const invoke = (args: Record<string, unknown>) =>
            tools.invoke(AUTOMATIONS_TOOL_NAME, args);
          const execution = async (method: string, params: Record<string, unknown>) => {
            const result = await withPreparedEmbeddedGatewayTools(
              {
                admittedRunContext: admitted,
                agentId: "main",
                sessionKey: SESSION,
                sessionId: SESSION_ID,
                agentHarnessId: "openclaw",
              },
              () => callGatewayTool(method, {}, params),
            );
            if (method === "cron.add" && loseExecutionReply) {
              loseExecutionReply = false;
              throw new Error("fixture discards actual native execution response");
            }
            return result;
          };
          const control = async (method: string, params: Record<string, unknown>) => {
            const response = await new Promise<unknown>((resolve, reject) => {
              const handler = expectDefined(cronHandlers[method], "host replay method");
              void Promise.resolve(
                handler({
                  req: { type: "req", id: "journal-host-control", method, params },
                  params,
                  respond: (ok, payload, error) => {
                    if (ok) {
                      resolve(payload);
                    } else {
                      reject(new Error(error?.message ?? "host control rejected"));
                    }
                  },
                  context: fixture.context,
                  client: createSyntheticPluginRuntimeClient({ scopes: ["operator.admin"] }),
                  isWebchatConnect: () => false,
                  hasCurrentClientAuthority: () => true,
                }),
              ).catch(reject);
            });
            if (method === "cron.replay.admit" && loseAdmissionReply) {
              loseAdmissionReply = false;
              throw new Error("fixture discards actual native admission response");
            }
            return response;
          };
          const start = () =>
            startHostAdmissionService({
              harnessRoot,
              database,
              target: "ws://127.0.0.1:18789",
              authority: `agent:main:${SESSION}`,
              control,
              execution,
              tool: async (method, params) => {
                expect(method).toBe(AUTOMATIONS_TOOL_NAME);
                prepareCalls += 1;
                return details(await invoke(params));
              },
            });
          host = await start();
          const proposal = await host.request("plan_tool_add", {
            ...reference,
            raw_tool_job: rawJob,
            max_attempts: 1,
          });
          expect(isRecord(proposal)).toBe(true);
          if (!isRecord(proposal)) {
            throw new Error("expected host proposal");
          }
          expect(prepareCalls).toBe(1);
          expect(await fixture.read()).toEqual([]);
          const admission = {
            proposal,
            review_digest: proposal.reviewDigest,
            evidence: "host-reviewed journal integration",
          };
          await expect(host.request("admit", admission)).rejects.toThrow("retain identity");
          expect(loseAdmissionReply).toBe(false);
          expect(await host.close()).toBe(0);
          host = await start();
          expect(await host.request("admit", admission)).toEqual({ state: "prepared" });
          expect(await host.request("inspect", reference)).toEqual({ state: "prepared" });
          const request = await host.request("tool_request", reference);
          if (!isRecord(request)) {
            throw new Error("expected admitted tool request");
          }
          expect(request).toMatchObject({
            action: "add",
            replay: { operationId: reference.operation, effectId: reference.effect, attempt: 1 },
          });
          await expect(host.request("dispatch", reference)).rejects.toThrow("retain identity");
          expect(loseExecutionReply).toBe(false);
          expect(await host.close()).toBe(0);
          host = await start();
          const first = details(await invoke(request));
          expect(details(await invoke(request))).toEqual(first);
          expect(await fixture.read()).toMatchObject([{ name: rawJob.name }]);
          expect(await fixture.read()).toHaveLength(1);
          expect(await host.close()).toBe(0);
          host = await start();
          expect(await host.request("inspect", reference)).toEqual({
            state: "accepted",
            result: first,
          });
          expect(await host.request("tool_request", reference)).toEqual(request);
          const changed = await host.request("plan_tool_add", {
            ...reference,
            raw_tool_job: { ...rawJob, name: "Changed reviewed intent" },
            max_attempts: 1,
          });
          if (!isRecord(changed)) {
            throw new Error("expected changed proposal");
          }
          await expect(
            host.request("admit", {
              ...admission,
              proposal: changed,
              review_digest: changed.reviewDigest,
            }),
          ).rejects.toThrow("retain identity");
          expect(await fixture.read()).toHaveLength(1);
        } finally {
          clearGatewayContextResolver(admitted);
        }
      });
      // The service remains live, but its retained tool callback cannot revive the run.
      await expect(
        expectDefined(host, "host service").request("plan_tool_add", {
          ...reference,
          effect: "retired",
          raw_tool_job: rawJob,
        }),
      ).rejects.toThrow("retain identity");
      expect(await fixture.read()).toHaveLength(1);
      expect((await fs.stat(database)).mode & 0o077).toBe(0);
    } finally {
      await host?.close();
    }
  },
);
