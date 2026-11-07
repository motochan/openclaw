import { createHash } from "node:crypto";
import { stableStringify } from "@openclaw/normalization-core";
import type { CronAddResult } from "./service/state.js";

export type CronReplayRef = { operationId: string; effectId: string; attempt: number };
export type CronReplayIntent =
  | { action: "run"; jobId: string; mode: "due" | "force" }
  | { action: "add"; intentHash: string };
export type CronReplayRequest = {
  ref: CronReplayRef;
  authorityId: string;
  intent: CronReplayIntent;
};
export type CronReplayRunResult = { ok: true; enqueued: true; runId: string };
export type CronReplayResult = CronReplayRunResult | CronAddResult;
export type CronReplayOutcome =
  | { state: "prepared" }
  | { state: "accepted"; result: CronReplayResult }
  | { state: "not-applied" };

export function cronReplayIntentHash(value: unknown): string {
  return createHash("sha256").update(stableStringify(value)).digest("hex");
}

/** Store binding prevents equal logical IDs at different schedulers sharing a run ID. */
export function cronReplayRunId(storeKey: string, ref: CronReplayRef): string {
  return `cron-replay.${cronReplayIntentHash([storeKey, ref.operationId, ref.effectId, ref.attempt])}`;
}
