import { stableStringify } from "@openclaw/normalization-core";
import type { Selectable } from "kysely";
import {
  executeSqliteQuerySync,
  executeSqliteQueryTakeFirstSync,
  getNodeSqliteKysely,
} from "../../infra/kysely-sync.js";
import type { EngineConnection } from "../../infra/sql-connection.js";
import { createSqliteSchemaEnsurer } from "../../infra/sqlite-schema-ensure.js";
import type { DB as OpenClawStateDatabase } from "../../state/openclaw-state-db.generated.js";
import { getOpenClawStateFeatureSchemaSql } from "../../state/openclaw-state-feature-schema.js";
import type { CronReplayOutcome, CronReplayResult } from "../replay.js";
import type { CronReplayInput, CronReplayWorkerOperations } from "./replay.types.js";

type ReplayDatabase = Pick<OpenClawStateDatabase, "cron_replay_effects" | "cron_replay_attempts">;
type AttemptRow = Selectable<ReplayDatabase["cron_replay_attempts"]>;

export const ensureCronReplaySchema = createSqliteSchemaEnsurer(
  () =>
    ["cron_replay_effects", "cron_replay_attempts"]
      .map((table) => getOpenClawStateFeatureSchemaSql(table))
      .join("\n"),
  { tables: ["cron_replay_effects", "cron_replay_attempts"] },
);

function key(input: CronReplayInput) {
  return {
    store_key: input.storeKey,
    operation_id: input.replay.ref.operationId,
    effect_id: input.replay.ref.effectId,
  };
}
function binding(input: CronReplayInput) {
  return stableStringify({
    adapter: "cron-replay/v1",
    authorityId: input.replay.authorityId,
    intent: input.replay.intent,
  });
}
function effect(db: EngineConnection, input: CronReplayInput) {
  const id = key(input);
  try {
    return executeSqliteQueryTakeFirstSync(
      db,
      getNodeSqliteKysely<ReplayDatabase>(db)
        .selectFrom("cron_replay_effects")
        .selectAll()
        .where("store_key", "=", id.store_key)
        .where("operation_id", "=", id.operation_id)
        .where("effect_id", "=", id.effect_id),
    );
  } catch (error) {
    // Inspection before first admission must not activate the lazy feature schema.
    if (error instanceof Error && error.message === "no such table: cron_replay_effects") {
      return undefined;
    }
    throw error;
  }
}
function attempt(db: EngineConnection, input: CronReplayInput, number = input.replay.ref.attempt) {
  const id = key(input);
  return executeSqliteQueryTakeFirstSync(
    db,
    getNodeSqliteKysely<ReplayDatabase>(db)
      .selectFrom("cron_replay_attempts")
      .selectAll()
      .where("store_key", "=", id.store_key)
      .where("operation_id", "=", id.operation_id)
      .where("effect_id", "=", id.effect_id)
      .where("attempt", "=", number),
  );
}
function requireEffect(db: EngineConnection, input: CronReplayInput) {
  const row = effect(db, input);
  if (!row || row.binding_json !== binding(input)) {
    throw new TypeError("cron replay admission missing or intent/authority conflicts");
  }
  return row;
}
function outcome(row: AttemptRow): CronReplayOutcome {
  if (row.state === "accepted" && row.result_json !== null) {
    return { state: "accepted", result: JSON.parse(row.result_json) as CronReplayResult };
  }
  if (row.state === "prepared" || row.state === "not-applied") {
    return { state: row.state };
  }
  throw new TypeError("invalid persisted cron replay outcome");
}
function grant(db: EngineConnection, input: CronReplayInput, evidence: string, nowMs: number) {
  executeSqliteQuerySync(
    db,
    getNodeSqliteKysely<ReplayDatabase>(db)
      .insertInto("cron_replay_attempts")
      .values({
        ...key(input),
        attempt: input.replay.ref.attempt,
        grant_evidence: evidence,
        granted_at_ms: nowMs,
        state: "prepared",
        result_json: null,
      }),
  );
}

/** Trusted host admission; transport dispatch never creates an admission. */
export function admitCronReplayInDatabase(
  db: EngineConnection,
  input: CronReplayWorkerOperations["cron.replayAdmit"]["input"],
): CronReplayOutcome {
  if (
    input.replay.ref.attempt !== 1 ||
    !Number.isInteger(input.maxAttempts) ||
    input.maxAttempts < 1 ||
    input.maxAttempts > 3 ||
    !input.evidence.trim()
  ) {
    throw new TypeError("cron replay admission requires attempt 1, evidence and a budget of 1..3");
  }
  ensureCronReplaySchema(db);
  const old = effect(db, input);
  if (old) {
    requireEffect(db, input);
    if (old.max_attempts !== input.maxAttempts || old.admission_evidence !== input.evidence) {
      throw new TypeError("cron replay admission is immutable");
    }
    return inspectCronReplayInDatabase(db, input);
  }
  executeSqliteQuerySync(
    db,
    getNodeSqliteKysely<ReplayDatabase>(db)
      .insertInto("cron_replay_effects")
      .values({
        ...key(input),
        binding_json: binding(input),
        max_attempts: input.maxAttempts,
        admission_evidence: input.evidence,
        admitted_at_ms: input.nowMs,
      }),
  );
  grant(db, input, input.evidence, input.nowMs);
  return { state: "prepared" };
}

/** No schema creation, grant creation, or mutation on the inspection path. */
export function inspectCronReplayInDatabase(
  db: EngineConnection,
  input: CronReplayInput,
): CronReplayOutcome {
  const admitted = requireEffect(db, input);
  const number = input.replay.ref.attempt;
  if (!Number.isInteger(number) || number < 1 || number > admitted.max_attempts) {
    throw new TypeError("cron replay attempt outside admitted budget");
  }
  const row = attempt(db, input);
  if (!row) {
    throw new TypeError("cron replay attempt is not authorized");
  }
  return outcome(row);
}

export function closeCronReplayInDatabase(
  db: EngineConnection,
  input: CronReplayInput,
): CronReplayOutcome {
  const current = inspectCronReplayInDatabase(db, input);
  if (current.state !== "prepared") {
    return current;
  }
  settle(db, input, "not-applied", null);
  return { state: "not-applied" };
}

export function retryCronReplayInDatabase(
  db: EngineConnection,
  input: CronReplayWorkerOperations["cron.replayRetry"]["input"],
): CronReplayOutcome {
  const admitted = requireEffect(db, input);
  const number = input.replay.ref.attempt;
  if (
    !Number.isInteger(number) ||
    number < 2 ||
    number > admitted.max_attempts ||
    !input.evidence.trim()
  ) {
    throw new TypeError("cron replay retry requires evidence and remaining budget");
  }
  for (let prior = 1; prior < number; prior++) {
    if (attempt(db, input, prior)?.state !== "not-applied") {
      throw new TypeError("cron replay retry requires fenced non-execution of every prior attempt");
    }
  }
  const existing = attempt(db, input);
  if (existing) {
    if (existing.grant_evidence !== input.evidence) {
      throw new TypeError("cron replay retry review is immutable");
    }
    return outcome(existing);
  }
  grant(db, input, input.evidence, input.nowMs);
  return { state: "prepared" };
}

function settle(
  db: EngineConnection,
  input: CronReplayInput,
  state: "accepted" | "not-applied",
  result: CronReplayResult | null,
) {
  const id = key(input);
  executeSqliteQuerySync(
    db,
    getNodeSqliteKysely<ReplayDatabase>(db)
      .updateTable("cron_replay_attempts")
      .set({ state, result_json: result === null ? null : JSON.stringify(result) })
      .where("store_key", "=", id.store_key)
      .where("operation_id", "=", id.operation_id)
      .where("effect_id", "=", id.effect_id)
      .where("attempt", "=", input.replay.ref.attempt)
      .where("state", "=", "prepared"),
  );
}

/** Must share the transaction that commits the scheduler effect. */
export function acceptCronReplayInDatabase(
  db: EngineConnection,
  input: CronReplayInput,
  result: CronReplayResult,
): void {
  settle(db, input, "accepted", result);
}
