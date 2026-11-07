import type { SqliteWorkerCommand } from "../../infra/sqlite-worker-contract.js";
import { requestSqliteWorkerOperationAdmission } from "../../infra/sqlite-worker-operation-admission.js";
import { getSqliteWorkerStateContext } from "../../infra/sqlite-worker-state-context.js";
import type { OpenClawStateDatabase } from "../../state/openclaw-state-db-contract.js";
import { runOpenClawStateWriteTransaction } from "../../state/openclaw-state-db.js";
import {
  admitCronReplayInDatabase,
  closeCronReplayInDatabase,
  inspectCronReplayInDatabase,
  retryCronReplayInDatabase,
} from "./replay.kernel.js";
import type { CronReplayWorkerOperations } from "./replay.types.js";

export function executeCronReplayInWorker(
  database: OpenClawStateDatabase,
  command: SqliteWorkerCommand<CronReplayWorkerOperations>,
) {
  if (command.type === "cron.replayInspect") {
    return inspectCronReplayInDatabase(database.db, command.input);
  }
  return runOpenClawStateWriteTransaction(
    ({ db }) => {
      requestSqliteWorkerOperationAdmission({ stage: "transaction", facts: undefined });
      const result =
        command.type === "cron.replayAdmit"
          ? admitCronReplayInDatabase(db, command.input)
          : command.type === "cron.replayRetry"
            ? retryCronReplayInDatabase(db, command.input)
            : closeCronReplayInDatabase(db, command.input);
      requestSqliteWorkerOperationAdmission({ stage: "commit", facts: undefined });
      return result;
    },
    { database, path: database.path, env: getSqliteWorkerStateContext().environment },
    { operationLabel: command.type },
  );
}
