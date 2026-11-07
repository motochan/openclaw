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
