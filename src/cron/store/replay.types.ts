import type { CronReplayOutcome, CronReplayRequest } from "../replay.js";

export type CronReplayInput = { storeKey: string; replay: CronReplayRequest };
export type CronReplayWorkerOperations = {
  "cron.replayAdmit": {
    input: CronReplayInput & { maxAttempts: number; evidence: string; nowMs: number };
    output: CronReplayOutcome;
  };
  "cron.replayInspect": { input: CronReplayInput; output: CronReplayOutcome };
  "cron.replayClose": { input: CronReplayInput; output: CronReplayOutcome };
  "cron.replayRetry": {
    input: CronReplayInput & { evidence: string; nowMs: number };
    output: CronReplayOutcome;
  };
};
