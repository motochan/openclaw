-- Generated runtime DDL; the owner supplies search_path and the transaction.

CREATE TABLE "cron_replay_effects" (
  "store_key" text COLLATE "C" NOT NULL,
  "operation_id" text COLLATE "C" NOT NULL,
  "effect_id" text COLLATE "C" NOT NULL,
  "binding_json" text COLLATE "C" NOT NULL,
  "max_attempts" bigint NOT NULL,
  "admission_evidence" text COLLATE "C" NOT NULL,
  "admitted_at_ms" bigint NOT NULL,
  PRIMARY KEY ("store_key", "operation_id", "effect_id"),
  CHECK (("max_attempts" BETWEEN 1 AND 3))
);
