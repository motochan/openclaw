import type { CronReplayRef } from "../../../packages/gateway-protocol/src/schema/cron.types.js";
import { CronCliError } from "./cron-cli-error.js";

/** Only forward identities from a prior host admission; never create them in a CLI retry. */
export function parseCronReplayOptions(opts: Record<string, unknown>): CronReplayRef | undefined {
  if (opts.operationId === undefined && opts.effectId === undefined && opts.attempt === undefined) {
    return undefined;
  }
  const identity = (value: unknown): string => {
    if (typeof value !== "string" || !value || value !== value.trim() || value.length > 256) {
      throw new CronCliError(
        "Replay requires both --operation-id and --effect-id from a host admission",
      );
    }
    return value;
  };
  const operationId = identity(opts.operationId);
  const effectId = identity(opts.effectId);
  const rawAttempt = opts.attempt ?? "1";
  if (typeof rawAttempt !== "string" || !/^[1-3]$/.test(rawAttempt)) {
    throw new CronCliError("--attempt must be 1, 2, or 3 from the reviewed admission");
  }
  return {
    operationId,
    effectId,
    attempt: Number(rawAttempt),
  };
}
