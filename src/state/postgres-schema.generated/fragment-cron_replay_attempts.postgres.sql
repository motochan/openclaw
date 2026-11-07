-- Generated runtime DDL; the owner supplies search_path and the transaction.

CREATE TABLE "cron_replay_attempts" (
  "store_key" text COLLATE "C" NOT NULL,
  "operation_id" text COLLATE "C" NOT NULL,
  "effect_id" text COLLATE "C" NOT NULL,
  "attempt" bigint NOT NULL,
  "grant_evidence" text COLLATE "C" NOT NULL,
  "granted_at_ms" bigint NOT NULL,
  "state" text COLLATE "C" NOT NULL,
  "result_json" text COLLATE "C",
  PRIMARY KEY ("store_key", "operation_id", "effect_id", "attempt"),
  CHECK (("attempt" BETWEEN 1 AND 3)),
  CHECK (("state" IN ('prepared', 'accepted', 'not-applied'))),
  CHECK ((("state" = 'accepted') AND "result_json" IS NOT NULL) OR (("state" != 'accepted') AND "result_json" IS NULL))
);

ALTER TABLE "cron_replay_attempts" ADD FOREIGN KEY ("store_key", "operation_id", "effect_id") REFERENCES "cron_replay_effects" ("store_key", "operation_id", "effect_id") ON UPDATE NO ACTION ON DELETE NO ACTION NOT DEFERRABLE;
