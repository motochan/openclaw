// Explicit Docker qualification: requires an already present image pinned by digest.
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import type { ServerResponse } from "node:http";
import path from "node:path";
import { expect, it } from "vitest";
import { runManagedCommand } from "../scripts/lib/managed-child-process.mts";
import type { OpenClawConfig } from "../src/config/types.openclaw.js";
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

const SESSION = "agent:main:credential-isolation";
const COMPLETE = "CREDENTIAL_ISOLATION_QUALIFIED";
const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";

async function command(bin: string, args: string[]): Promise<string> {
  let stdout = "";
  let stderr = "";
  const code = await runManagedCommand({
    bin,
    args,
    timeoutMs: 30_000,
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
  if (code !== 0) {
    throw new Error(`${bin} qualification command failed: ${stderr}`);
  }
  return stdout;
}

function tool(response: ServerResponse, id: number, name: string, params: Record<string, unknown>) {
  const item = {
    type: "function_call",
    id: `fc_isolation_${id}`,
    call_id: `call_isolation_${id}`,
    name,
    arguments: JSON.stringify(params),
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
      output_index: 0,
      item_id: item.id,
      arguments: item.arguments,
    },
    { type: "response.output_item.done", output_index: 0, item },
    {
      type: "response.completed",
      response: {
        id: `resp_isolation_${id}`,
        status: "completed",
        output: [item],
        usage: { input_tokens: 10, output_tokens: 2, total_tokens: 12 },
      },
    },
  ]);
}

function output(body: Record<string, unknown>, id: number): string {
  if (!Array.isArray(body.input)) {
    throw new Error("model input missing");
  }
  const value = body.input.findLast(
    (entry) =>
      isRecord(entry) &&
      entry.type === "function_call_output" &&
      entry.call_id === `call_isolation_${id}`,
  );
  if (!isRecord(value) || typeof value.output !== "string") {
    throw new Error("tool output missing");
  }
  return value.output;
}

it.runIf(process.env.OPENCLAW_ISOLATION_IMAGE !== undefined)(
  "isolates host credentials and journal from actual agent shell while retaining admitted automations",
  { timeout: 180_000 },
  async () => {
    const harnessRoot = process.env.JAMIE_HARNESS_ROOT;
    if (!harnessRoot || !path.isAbsolute(harnessRoot)) {
      throw new Error("JAMIE_HARNESS_ROOT must identify the reviewed host adapter checkout");
    }
    const image = process.env.OPENCLAW_ISOLATION_IMAGE;
    if (!image || !/^sha256:[a-f0-9]{64}$/.test(image)) {
      throw new Error("OPENCLAW_ISOLATION_IMAGE must pin an existing local Docker image ID");
    }
    expect(
      (await command("docker", ["image", "inspect", "--format", "{{.Id}}", image])).trim(),
    ).toBe(image);
    // Keep prefix + native workspace slug below the container-name shortening limit.
    const prefix = `ci${randomUUID().slice(0, 8)}-`;
    const hostOnly = `synthetic-host-only-${randomUUID()}`;
    const reviewed = createDeferred<Record<string, unknown>>();
    const admitted = createDeferred();
    const failed = createDeferred<never>();
    void failed.promise.catch(() => undefined);
    const requests: Record<string, unknown>[] = [];
    let step = 0;
    let probe = "";
    let marker = "";
    let secretPath = "";
    let preparedJob: Record<string, unknown>;
    let preparedIntent: unknown;
    let firstJob: unknown;
    const replay = { operationId: "sandbox-admitted-op", effectId: "add", attempt: 1 };
    const job = {
      name: "Sandbox admitted automation",
      enabled: false,
      schedule: { kind: "every", everyMs: 86_400_000 },
      sessionTarget: "current",
      payload: { kind: "agentTurn", message: "Synthetic isolated job" },
      delivery: { mode: "none" },
    };
    const model = await startOpenAiResponsesModelServer({
      modelId: "isolation-proof",
      bindError: "isolation model did not bind",
      onRequest: async (request, response) => {
        try {
          const chunks: Buffer[] = [];
          for await (const chunk of request) {
            chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
          }
          const body: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          if (!isRecord(body)) {
            throw new Error("invalid model request");
          }
          requests.push(body);
          const attempt = `printf escaped > ${quote(marker)}`;
          switch (step++) {
            case 0:
              tool(response, 0, "exec", { command: probe, yieldMs: 10000 });
              break;
            case 1:
              expect(output(body, 0)).toMatch(/(?:^|\n)ISOLATION_PROBE_PASSED(?:\n|$)/);
              expect(output(body, 0)).not.toContain("Traceback");
              tool(response, 1, "read", { path: secretPath });
              break;
            case 2:
              expect(JSON.parse(output(body, 1))).toMatchObject({ status: "error", tool: "read" });
              tool(response, 2, "exec", { command: attempt, host: "gateway" });
              break;
            case 3:
              expect(output(body, 2)).toContain("host: must be equal to one of the allowed values");
              tool(response, 3, "exec", { command: attempt, host: "node" });
              break;
            case 4:
              expect(output(body, 3)).toContain("host: must be equal to one of the allowed values");
              tool(response, 4, "exec", { command: attempt, elevated: true });
              break;
            case 5:
              expect(output(body, 4)).toContain("elevated is not available");
              tool(response, 5, "automations", {
                action: "prepare_add",
                job: { ...job, payload: { kind: "command", argv: ["sh", "-c", attempt] } },
              });
              break;
            case 6:
              expect(output(body, 5)).toContain(
                "job.payload.kind: must be equal to one of the allowed values",
              );
              tool(response, 6, "automations", {
                action: "prepare_add",
                job: { ...job, schedule: { kind: "stream", command: ["sh", "-c", attempt] } },
              });
              break;
            case 7:
              expect(output(body, 6)).toContain(
                "job.schedule.kind: must be equal to one of the allowed values",
              );
              tool(response, 7, "automations", {
                action: "prepare_add",
                job: { ...job, payload: { kind: "script", script: attempt } },
              });
              break;
            case 8:
              expect(output(body, 7)).toContain(
                "job.payload.kind: must be equal to one of the allowed values",
              );
              tool(response, 8, "automations", { action: "prepare_add", job });
              break;
            case 9: {
              const prepared: unknown = JSON.parse(output(body, 8));
              if (!isRecord(prepared)) {
                throw new Error("invalid prepared job");
              }
              if (!isRecord(prepared.executionParams)) {
                throw new Error("missing final job");
              }
              preparedJob = prepared.executionParams;
              preparedIntent = prepared.intent;
              tool(response, 9, "automations", { action: "add", job: preparedJob, replay });
              break;
            }
            case 10:
              expect(output(body, 9)).toContain("host review required");
              reviewed.resolve({});
              await admitted.promise;
              tool(response, 10, "automations", { action: "add", job: preparedJob, replay });
              break;
            case 11:
              firstJob = JSON.parse(output(body, 10));
              expect(firstJob).toMatchObject({
                name: job.name,
                owner: { agentId: "main", sessionKey: SESSION },
              });
              tool(response, 11, "automations", { action: "add", job: preparedJob, replay });
              break;
            case 12:
              expect(JSON.parse(output(body, 11))).toEqual(firstJob);
              tool(response, 12, "automations", {
                action: "add",
                job: { ...preparedJob, name: "Unreviewed changed job" },
                replay,
              });
              break;
            case 13:
              expect(output(body, 12)).toContain("conflict");
              tool(response, 13, "exec", { command: probe, yieldMs: 10000 });
              break;
            case 14:
              expect(output(body, 13)).toContain("ISOLATION_PROBE_PASSED");
              writeOpenAiResponsesText(response, {
                text: COMPLETE,
                messageId: "msg_isolation",
                responseId: "resp_isolation_done",
              });
              break;
            case 15:
              tool(response, 15, "automations", { action: "add", job: preparedJob, replay });
              break;
            case 16:
              expect(JSON.parse(output(body, 15))).toEqual(firstJob);
              writeOpenAiResponsesText(response, {
                text: COMPLETE,
                messageId: "msg_isolation_restart",
                responseId: "resp_isolation_restart_done",
              });
              break;
            default:
              throw new Error("unexpected model request");
          }
        } catch (error) {
          failed.reject(error);
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
        providerId: "isolation-proof",
        modelId: "isolation-proof",
        reasoning: false,
        toolProfile: "coding",
      });
      config.agents = {
        ...config.agents,
        defaults: {
          ...config.agents?.defaults,
          sandbox: {
            mode: "all",
            backend: "docker",
            scope: "session",
            workspaceAccess: "none",
            browser: { enabled: false },
            docker: {
              image,
              containerPrefix: prefix,
              network: "none",
              readOnlyRoot: true,
              user: "1000:1000",
              capDrop: ["ALL"],
              tmpfs: ["/tmp", "/var/tmp", "/run"],
            },
          },
        },
      };
      const allow = ["exec", "read", "automations"];
      config.tools = {
        profile: "coding",
        codeMode: false,
        toolSearch: false,
        allow,
        exec: { host: "sandbox" },
        elevated: { enabled: false },
        fs: { workspaceOnly: true },
        sandbox: { tools: { allow } },
      };
      config.cron = { enabled: false, triggers: { enabled: false } };
      instance = await createOpenClawTestInstance({
        name: "credential-isolation",
        config,
        env: {
          OPENCLAW_SKIP_PROVIDERS: undefined,
          OPENCLAW_TEST_MINIMAL_GATEWAY: undefined,
          ISOLATION_HOST_ONLY: hostOnly,
        },
      });
      secretPath = instance.configPath;
      marker = path.join(instance.homeDir, "escaped-marker");
      const journal = path.join(instance.homeDir, "host-admission.sqlite");
      const launcher = path.join(instance.homeDir, "candidate-cli");
      await fs.writeFile(
        launcher,
        `#!/bin/sh\nexec ${quote(process.execPath)} ${quote(path.resolve("dist/index.js"))} "$@"\n`,
        { mode: 0o700 },
      );
      const effectiveConfig = JSON.parse(
        await fs.readFile(instance.configPath, "utf8"),
      ) as OpenClawConfig;
      effectiveConfig.plugins = {
        ...effectiveConfig.plugins,
        enabled: true,
        allow: ["scheduler-admission", "openai"],
        load: { paths: [path.join(harnessRoot, "plugins/scheduler-admission")] },
        entries: {
          "scheduler-admission": {
            enabled: true,
            hooks: { allowConversationAccess: true },
            config: {
              repoDir: harnessRoot,
              database: journal,
              target: instance.url,
              executable: launcher,
            },
          },
        },
      };
      await fs.writeFile(instance.configPath, JSON.stringify(effectiveConfig));
      await command("python3", [
        "-c",
        "import sys; sys.path.insert(0,sys.argv[1]); from harness.scheduler_admission import HostAdmission; HostAdmission(sys.argv[2],target=sys.argv[3],authority=sys.argv[4],control_rpc=None,execution_rpc=None)",
        harnessRoot,
        journal,
        instance.url,
        `agent:main:${SESSION}`,
      ]);
      const configBefore = await fs.readFile(secretPath);
      const paths = [
        secretPath,
        journal,
        `/proc/${process.pid}/root${secretPath}`,
        `/proc/1/root${secretPath}`,
      ];
      const python = `import os,json,socket,sqlite3,subprocess,sys
paths=json.loads(sys.argv[1])
for p in paths:
 try:
  open(p,'rb').read(1)
 except (FileNotFoundError,PermissionError,NotADirectoryError): pass
 else: raise AssertionError('host path readable')
try:
 open(sys.argv[2],'r+b')
except (FileNotFoundError,PermissionError): pass
else: raise AssertionError('journal writable')
try:
 c=sqlite3.connect('file:'+sys.argv[2]+'?mode=rw',uri=True); c.execute("INSERT INTO grants VALUES ('unreviewed','add',1,'unreviewed')"); c.commit()
except sqlite3.OperationalError: pass
else: raise AssertionError('journal mutation succeeded')
assert not os.path.exists('/var/run/docker.sock')
assert 'ISOLATION_HOST_ONLY' not in os.environ
assert not any(k in os.environ for k in ['OPENCLAW_GATEWAY_TOKEN','OPENCLAW_GATEWAY_PASSWORD','DOCKER_HOST'])
for host,port in [('127.0.0.1',int(sys.argv[3])),('198.51.100.1',443)]:
 s=socket.socket(); s.settimeout(1)
 try: s.connect((host,port))
 except OSError: pass
 else: raise AssertionError('raw RPC network reachable')
 finally: s.close()
child=subprocess.run([sys.executable,'-c',"import sys;open(sys.argv[1],'rb')",paths[0]],capture_output=True)
assert child.returncode != 0
link='/tmp/host-secret-link'
if os.path.lexists(link): os.unlink(link)
os.symlink(paths[0],link)
try: open(link,'rb')
except (FileNotFoundError,PermissionError): pass
else: raise AssertionError('symlink escaped')
assert subprocess.run(['sh','-c','test ! -r "$1"','sh',paths[0]]).returncode == 0
print('ISOLATION_PROBE_PASSED')
`;
      probe = `python3 -c ${quote(python)} ${quote(JSON.stringify(paths))} ${quote(journal)} ${instance.port}`;
      await instance.startGateway();
      client = await connectGatewayClient({
        url: instance.url,
        token: instance.gatewayToken,
        scopes: ["operator.admin", "operator.read", "operator.write"],
      });
      pending = client.request(
        "agent",
        {
          sessionKey: SESSION,
          deliver: false,
          idempotencyKey: randomUUID(),
          message:
            "Run the synthetic isolation probe, then prepare the reviewed automation and wait for host admission.",
        },
        { expectFinal: true, timeoutMs: 120_000 },
      );
      void pending.catch(() => undefined);
      await Promise.race([
        reviewed.promise,
        failed.promise,
        pending.then(() => {
          throw new Error("turn ended before review");
        }),
      ]);
      const proposals = await client.request<unknown>("scheduler.admission.pending", {});
      expect(Array.isArray(proposals)).toBe(true);
      if (!Array.isArray(proposals) || proposals.length !== 1 || !isRecord(proposals[0])) {
        throw new Error("expected one host review proposal");
      }
      const proposal = proposals[0];
      expect(proposal.plan).toMatchObject({
        authorityId: `agent:main:${SESSION}`,
        params: preparedJob!,
        intent: preparedIntent,
        operationId: replay.operationId,
        effectId: replay.effectId,
      });
      const reader = await connectGatewayClient({
        url: instance.url,
        token: instance.gatewayToken,
        scopes: ["operator.read"],
      });
      try {
        await expect(reader.request("scheduler.admission.pending", {})).rejects.toThrow();
        await expect(
          reader.request("scheduler.admission.approve", {
            reviewDigest: proposal.reviewDigest,
            evidence: "reader-cannot-approve",
          }),
        ).rejects.toThrow();
      } finally {
        await disconnectGatewayClient(reader);
      }
      await expect(
        client.request("scheduler.admission.approve", {
          reviewDigest: "0".repeat(64),
          evidence: "wrong-digest",
        }),
      ).rejects.toThrow();
      await client.request("scheduler.admission.approve", {
        reviewDigest: proposal.reviewDigest,
        evidence: "host-isolation-review",
      });
      admitted.resolve();
      expect(await Promise.race([pending, failed.promise])).toMatchObject({
        status: "ok",
        result: { payloads: expect.arrayContaining([expect.objectContaining({ text: COMPLETE })]) },
      });
      expect(step).toBe(15);
      await disconnectGatewayClient(client);
      client = undefined;
      await instance.stopGateway();
      await instance.startGateway();
      client = await connectGatewayClient({
        url: instance.url,
        token: instance.gatewayToken,
        scopes: ["operator.admin", "operator.read", "operator.write"],
      });
      expect(await client.request("scheduler.admission.pending", {})).toEqual([]);
      pending = client.request(
        "agent",
        {
          sessionKey: SESSION,
          deliver: false,
          idempotencyKey: randomUUID(),
          message: "Retry the same reviewed automation after Gateway replacement.",
        },
        { expectFinal: true, timeoutMs: 120_000 },
      );
      expect(await Promise.race([pending, failed.promise])).toMatchObject({ status: "ok" });
      expect(step).toBe(17);
      const rows = JSON.parse(
        await command("python3", [
          "-c",
          "import json,sqlite3,sys; c=sqlite3.connect(sys.argv[1]); print(json.dumps({'effects':c.execute('SELECT count(*) FROM effects').fetchone()[0],'grants':c.execute('SELECT operation,effect,attempt,evidence FROM grants').fetchall()})); c.close()",
          journal,
        ]),
      );
      expect(rows).toEqual({
        effects: 1,
        grants: [[replay.operationId, replay.effectId, 1, "host-isolation-review"]],
      });
      expect(await fs.readFile(secretPath)).toEqual(configBefore);
      await expect(fs.stat(marker)).rejects.toMatchObject({ code: "ENOENT" });
      const serialized = JSON.stringify(requests);
      expect(serialized).not.toContain(instance.gatewayToken);
      expect(serialized).not.toContain(hostOnly);
      expect(serialized).not.toContain("host-isolation-review");
      const ids = (await command("docker", ["ps", "-aq", "--filter", `name=^/${prefix}`]))
        .trim()
        .split(/\s+/)
        .filter(Boolean);
      expect(ids).toHaveLength(1);
      const containers: unknown = JSON.parse(await command("docker", ["inspect", ...ids]));
      expect(containers).toMatchObject([
        {
          Image: image,
          Config: { User: "1000:1000" },
          HostConfig: {
            NetworkMode: "none",
            ReadonlyRootfs: true,
            Privileged: false,
            PidMode: "",
            CapDrop: ["ALL"],
            SecurityOpt: expect.arrayContaining(["no-new-privileges"]),
          },
        },
      ]);
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
            const ids = (await command("docker", ["ps", "-aq", "--filter", `name=^/${prefix}`]))
              .trim()
              .split(/\s+/)
              .filter(Boolean);
            if (ids.length) {
              await command("docker", ["rm", "-f", ...ids]);
            }
          }
        }
      }
    }
  },
);
