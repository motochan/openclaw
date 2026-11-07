import type { ChildProcess } from "node:child_process";
import path from "node:path";
import { createInterface } from "node:readline";
import { runManagedCommand } from "../../../scripts/lib/managed-child-process.mts";
import { isRecord } from "../../utils.js";

type Callback = (method: string, params: Record<string, unknown>) => Promise<unknown>;

/** Private parent/child pipes; no listener or control capability is exported to tools. */
export async function startHostAdmissionService(options: {
  harnessRoot: string;
  database: string;
  target: string;
  authority: string;
  control: Callback;
  execution: Callback;
  tool: Callback;
}) {
  if (!path.isAbsolute(options.harnessRoot)) {
    throw new Error("JAMIE_HARNESS_ROOT must be absolute");
  }
  let child: ChildProcess | undefined;
  let nextId = 0;
  let pending:
    | { id: string; resolve: (value: unknown) => void; reject: (error: Error) => void }
    | undefined;
  let readyResolve!: () => void;
  let readyReject!: (error: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  const cancellation = new AbortController();
  const fail = (error: Error) => {
    readyReject(error);
    pending?.reject(error);
    pending = undefined;
  };
  const send = (value: unknown) => {
    if (!child?.stdin?.writable) {
      throw new Error("host admission service input is unavailable");
    }
    child.stdin.write(`${JSON.stringify(value)}\n`);
  };
  const receive = async (line: string) => {
    const message: unknown = JSON.parse(line);
    if (!isRecord(message)) {
      throw new Error("invalid host admission service message");
    }
    if (message.type === "ready") {
      readyResolve();
      return;
    }
    if (message.type === "call") {
      if (
        !pending ||
        typeof message.id !== "number" ||
        typeof message.method !== "string" ||
        !isRecord(message.params) ||
        (message.route !== "control" && message.route !== "execution" && message.route !== "tool")
      ) {
        throw new Error("invalid host admission callback");
      }
      try {
        const result = await options[message.route](message.method, message.params);
        send({ type: "return", id: message.id, succeeded: true, result });
      } catch {
        send({ type: "return", id: message.id, succeeded: false });
      }
      return;
    }
    if (message.type !== "result" || !pending || message.id !== pending.id) {
      throw new Error("unexpected host admission service result");
    }
    const current = pending;
    pending = undefined;
    if (message.succeeded === true) {
      current.resolve(message.result);
    } else {
      current.reject(
        new Error("Host admission operation failed; retain identity and inspect the same attempt."),
      );
    }
  };
  const closed = runManagedCommand({
    bin: "python3",
    args: [
      "-m",
      "harness.scheduler_admission_service",
      "--db",
      options.database,
      "--target",
      options.target,
      "--authority",
      options.authority,
    ],
    cwd: options.harnessRoot,
    env: { ...process.env, PYTHONPATH: options.harnessRoot, PYTHONUNBUFFERED: "1" },
    stdio: ["pipe", "pipe", "pipe"],
    timeoutMs: 30_000,
    signal: cancellation.signal,
    onReady: (started) => {
      child = started;
      if (!started.stdout) {
        throw new Error("host admission service output is unavailable");
      }
      // Drain private diagnostics without copying them to test reports.
      started.stderr?.resume();
      const lines = createInterface({ input: started.stdout });
      void (async () => {
        for await (const line of lines) {
          await receive(line);
        }
      })().catch(() => {
        fail(new Error("host admission service protocol failed"));
        cancellation.abort();
      });
    },
  }).then(
    (status) => {
      fail(new Error("host admission service exited"));
      return status;
    },
    () => {
      fail(new Error("host admission service process failed"));
      return -1;
    },
  );
  try {
    await ready;
  } catch (error) {
    cancellation.abort();
    await closed;
    throw error;
  }
  return {
    request(action: string, args: Record<string, unknown>): Promise<unknown> {
      if (pending) {
        throw new Error("host admission service requests must be sequential");
      }
      return new Promise((resolve, reject) => {
        const id = String(++nextId);
        pending = { id, resolve, reject };
        try {
          send({ type: "request", id, action, args });
        } catch {
          fail(new Error("host admission service request failed"));
        }
      });
    },
    async close() {
      child?.stdin?.end();
      return await closed;
    },
  };
}
