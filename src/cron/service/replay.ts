import { createSqliteWorkerWriteAdmission } from "../../infra/sqlite-worker-store.js";
import { runOpenClawStateWorkerOperation } from "../../state/openclaw-state-worker-store.js";
import type { CronReplayOutcome, CronReplayRequest } from "../replay.js";
import { locked } from "./locked.js";
import type { CronServiceState } from "./state.js";
import { captureCronServiceMutationSource } from "./store.js";

export type CronReplayControl =
  | { action: "admit"; replay: CronReplayRequest; maxAttempts: number; evidence: string }
  | { action: "inspect" | "close"; replay: CronReplayRequest }
  | { action: "retry"; replay: CronReplayRequest; evidence: string };

/** Invoked under the scheduler lock; the worker owns durable replay decisions. */
export async function inspectCronReplay(
  state: CronServiceState,
  replay: CronReplayRequest,
  assertCurrent?: () => void,
): Promise<CronReplayOutcome> {
  const source = captureCronServiceMutationSource(state);
  const result = await runOpenClawStateWorkerOperation(
    source.context,
    (scope) =>
      scope.execute({ type: "cron.replayInspect", input: { storeKey: source.storeKey, replay } }),
    {
      assertCurrent: () => {
        source.assertCurrent();
        assertCurrent?.();
      },
    },
  );
  source.assertCurrent();
  assertCurrent?.();
  return result;
}

export async function controlCronReplay(
  state: CronServiceState,
  command: CronReplayControl,
  commitGuard: () => void,
): Promise<CronReplayOutcome> {
  return await locked(state, async () => {
    if (command.action === "inspect") {
      return await inspectCronReplay(state, command.replay, commitGuard);
    }
    const source = captureCronServiceMutationSource(state);
    const assertCurrent = () => {
      source.assertCurrent();
      commitGuard();
    };
    const input = { storeKey: source.storeKey, replay: command.replay };
    return await runOpenClawStateWorkerOperation(
      source.context,
      (scope) => {
        switch (command.action) {
          case "inspect":
            throw new Error("Inspection was handled before write admission");
          case "admit":
            return scope.execute({
              type: "cron.replayAdmit",
              input: {
                ...input,
                evidence: command.evidence,
                maxAttempts: command.maxAttempts,
                nowMs: state.deps.nowMs(),
              },
            });
          case "close":
            return scope.execute({ type: "cron.replayClose", input });
          case "retry":
            return scope.execute({
              type: "cron.replayRetry",
              input: { ...input, evidence: command.evidence, nowMs: state.deps.nowMs() },
            });
        }
        throw new Error("Unknown cron replay control");
      },
      {
        assertCurrent,
        createAdmission: createSqliteWorkerWriteAdmission(assertCurrent, [
          source.context.admission.databasePath,
        ]),
      },
    );
  });
}
