// Local cross-repository qualification: requires JAMIE_HARNESS_ROOT explicitly.
import fs from "node:fs/promises";
import path from "node:path";
import { expect, it } from "vitest";
import { runManagedCommand } from "../scripts/lib/managed-child-process.mts";
import { createCronTool } from "../src/agents/tools/cron-tool.js";
import { setRuntimeConfigSnapshot, clearRuntimeConfigSnapshot } from "../src/config/config.js";
import { connectGatewayClient, disconnectGatewayClient } from "../src/gateway/test-helpers.e2e.js";
import { createOpenClawTestInstance } from "./helpers/openclaw-test-instance.js";

it(
  "qualifies host admission through authenticated RPC, CLI, tool, and Gateway replacement",
  { timeout: 180_000 },
  async () => {
    const harnessRoot = process.env.JAMIE_HARNESS_ROOT;
    if (!harnessRoot) {
      throw new Error("JAMIE_HARNESS_ROOT must identify the reviewed host adapter checkout");
    }
    const instance = await createOpenClawTestInstance({
      name: "native-host-replay",
      config: { cron: { enabled: false } },
    });
    let client: Awaited<ReturnType<typeof connectGatewayClient>> | undefined;
    let jobId: string;
    let settleFinished!: () => void;
    const finished = new Promise<void>((resolve) => {
      settleFinished = resolve;
    });
    try {
      await instance.startGateway();
      client = await connectGatewayClient({
        url: instance.url,
        token: instance.gatewayToken,
        scopes: ["operator.admin", "operator.read", "operator.write"],
        onEvent: (event) => {
          const payload = event.payload as { jobId?: string; action?: string } | undefined;
          if (event.event === "cron" && payload?.jobId === jobId && payload.action === "finished") {
            settleFinished();
          }
        },
      });
      const marker = path.join(instance.homeDir, "effect-count.txt");
      const created = await client.request<{ id: string }>("cron.add", {
        name: "host replay qualification",
        enabled: false,
        schedule: { kind: "every", everyMs: 86_400_000 },
        sessionTarget: "isolated",
        wakeMode: "next-heartbeat",
        delivery: { mode: "none" },
        payload: {
          kind: "command",
          argv: [
            process.execPath,
            "-e",
            "require('node:fs').appendFileSync(process.argv[1], 'effect\\n')",
            marker,
          ],
        },
      });
      jobId = created.id;
      expect(jobId).toEqual(expect.any(String));
      const launcher = path.join(instance.homeDir, "candidate-cli");
      const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
      await fs.writeFile(
        launcher,
        `#!/bin/sh\nexec ${quote(process.execPath)} ${quote(path.resolve("dist/index.js"))} "$@"\n`,
        { mode: 0o700 },
      );
      const journal = path.join(instance.homeDir, "host.sqlite");
      const code = `
import json, sys
from harness.scheduler_admission import GatewayCLI, HostAdmission
journal, launcher, target, job = sys.argv[1:]
rpc = GatewayCLI(launcher, target)
host = HostAdmission(journal, target=target, authority="operator", control_rpc=rpc, execution_rpc=rpc)
proposal = host.plan("qualified-operation", "run", "run", {"id":job,"mode":"force"})
host.admit(proposal, review_digest=proposal["reviewDigest"], evidence="qualification/review")
def lose_reply(method, params):
    rpc(method, params)
    raise TimeoutError("fixture discards response after real RPC returns")
host.execute = lose_reply
try:
    host.dispatch("qualified-operation", "run")
except TimeoutError:
    pass
else:
    raise AssertionError("lost reply was not exercised")
replacement = HostAdmission(journal, target=target, authority="operator", control_rpc=rpc, execution_rpc=rpc)
receipt = replacement.inspect("qualified-operation", "run")
assert receipt["state"] == "accepted", receipt
assert replacement.dispatch("qualified-operation", "run") == receipt
print(json.dumps(receipt))
`;
      let stdout = "";
      let stderr = "";
      const status = await runManagedCommand({
        bin: "python3",
        args: ["-c", code, journal, launcher, instance.url, jobId],
        cwd: harnessRoot,
        env: { ...instance.env, PYTHONPATH: harnessRoot },
        timeoutMs: 90_000,
        stdio: ["ignore", "pipe", "pipe"],
        onReady: (child) => {
          child.stdout?.on("data", (chunk) => {
            stdout += String(chunk);
          });
          child.stderr?.on("data", (chunk) => {
            stderr += String(chunk);
          });
        },
      });
      expect(status, stderr).toBe(0);
      const receipt = JSON.parse(stdout) as { result: { runId: string } };
      const replay = { operationId: "qualified-operation", effectId: "run", attempt: 1 };
      const cli = await instance.cli([
        "cron",
        "run",
        jobId,
        "--operation-id",
        replay.operationId,
        "--effect-id",
        replay.effectId,
        "--attempt",
        "1",
        "--json",
      ]);
      expect(cli.code, cli.stderr).toBe(0);
      expect(JSON.parse(cli.stdout)).toMatchObject(receipt.result);
      const activeClient = client;
      setRuntimeConfigSnapshot({ cron: { enabled: false } });
      const tool = createCronTool(
        { config: {} },
        {
          callGatewayTool: async (method, _options, params) =>
            await activeClient.request(method, params),
        },
      );
      const toolResult = await tool.execute("same-key-tool", {
        action: "run",
        jobId,
        runMode: "force",
        replay,
      });
      expect(toolResult.details).toMatchObject(receipt.result);
      await finished;
      // cron.runs is produced terminal scheduler state, independent of wrapper exit codes.
      const history = await client.request<{ entries: Array<{ runId: string; status: string }> }>(
        "cron.runs",
        { id: jobId, limit: 10 },
      );
      expect(history.entries.filter((entry) => entry.runId === receipt.result.runId)).toHaveLength(
        1,
      );
      expect(history.entries[0]?.status).toBe("ok");
      expect(await fs.readFile(marker, "utf8")).toBe("effect\n");
      await expect(
        client.request("cron.run", { id: jobId, mode: "due", replay }),
      ).rejects.toThrow();
      await expect(
        client.request("cron.run", {
          id: jobId,
          mode: "force",
          replay: { ...replay, effectId: "invented" },
        }),
      ).rejects.toThrow();
      const reader = await connectGatewayClient({
        url: instance.url,
        token: instance.gatewayToken,
        scopes: ["operator.read"],
      });
      try {
        await expect(
          reader.request("cron.replay.admit", {
            operationId: "reader-denied",
            effectId: "run",
            authorityId: "operator",
            intent: { action: "run", jobId, mode: "force" },
            evidence: "fixture",
            maxAttempts: 1,
          }),
        ).rejects.toThrow();
      } finally {
        await disconnectGatewayClient(reader);
      }
      // Explicitly demonstrate the unresolved raw-add/tool normalization gap.
      const rawAdd = {
        name: "raw-tool-add-mismatch",
        schedule: { kind: "at", at: "2099-01-01T00:00:00.000Z" },
        sessionTarget: "main",
        wakeMode: "now",
        payload: { kind: "systemEvent", text: "fixture" },
      };
      const prepared = await client.request<{ authorityId: string; intent: unknown }>(
        "cron.replay.prepareAdd",
        rawAdd,
      );
      const addReplay = { operationId: "qualified-operation", effectId: "raw-add", attempt: 1 };
      await client.request("cron.replay.admit", {
        operationId: addReplay.operationId,
        effectId: addReplay.effectId,
        authorityId: prepared.authorityId,
        intent: prepared.intent,
        evidence: "fixture/raw-add",
        maxAttempts: 1,
      });
      await expect(
        tool.execute("raw-add", { action: "add", job: rawAdd, replay: addReplay }),
      ).rejects.toThrow(/conflict/);
      await disconnectGatewayClient(client);
      client = undefined;
      await instance.stopGateway();
      await instance.startGateway();
      client = await connectGatewayClient({
        url: instance.url,
        token: instance.gatewayToken,
        scopes: ["operator.admin"],
      });
      expect(await client.request("cron.run", { id: jobId, mode: "force", replay })).toMatchObject(
        receipt.result,
      );
      expect(await fs.readFile(marker, "utf8")).toBe("effect\n");
    } finally {
      clearRuntimeConfigSnapshot();
      if (client) {
        await disconnectGatewayClient(client);
      }
      await instance.cleanup();
    }
  },
);
