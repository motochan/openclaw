import {
  validateCronAddParams,
  validateCronRunParams,
  validateCronReplayAdmitParams,
  validateCronReplayInspectParams,
  validateCronReplayCloseParams,
  validateCronReplayRetryParams,
} from "../../../packages/gateway-protocol/src/index.js";
import { cronAddResultReadView } from "../../cron/job-read-view.js";
import {
  cronReplayIntentHash,
  type CronReplayIntent,
  type CronReplayRef,
  type CronReplayRequest,
} from "../../cron/replay.js";
import type { CronReplayControl } from "../../cron/service/replay.js";
import type { CronJobCreate } from "../../cron/types.js";
import { formatErrorMessage } from "../../infra/errors.js";
import { isRecord } from "../../utils.js";
import { getCronManagementAuthority } from "../cron-creator-authority-grant.js";
import { getGatewayProcessInstanceId } from "../process-instance.js";
import {
  applyCronCreateCallerScopeDefault,
  readCronCallerScope,
  resolveCronMutationCommitGuard,
} from "./cron-caller-scope.js";
import { normalizeCronAddRequest } from "./cron-input-validation.js";
import {
  assertCronReadCurrent,
  respondInvalidCronParams,
  resolveCronJobId,
} from "./cron-job-access.js";
import type { GatewayRequestHandlerOptions, GatewayRequestHandlers } from "./types.js";
import { assertValidParams } from "./validation.js";

/** Operator credentials retain their existing shared administrative trust domain. */
function authorityId(client: GatewayRequestHandlerOptions["client"]): string {
  const identity = client?.internal?.agentRuntimeIdentity;
  if (identity && !identity.sessionKey) {
    throw new TypeError("cron replay requires a bound agent session");
  }
  return identity ? `agent:${identity.agentId}:${identity.sessionKey}` : "operator";
}

export function cronReplayRequest(
  client: GatewayRequestHandlerOptions["client"],
  ref: CronReplayRef | undefined,
  intent: CronReplayIntent,
): CronReplayRequest | undefined {
  const identity = client?.internal?.agentRuntimeIdentity;
  if (!ref) {
    if (identity) {
      throw new TypeError("cron agent mutation requires a host-admitted replay identity");
    }
    return undefined;
  }
  return { ref, intent, authorityId: authorityId(client) };
}

type ReplayReadOptions = Pick<
  GatewayRequestHandlerOptions,
  "client" | "context" | "signal" | "hasCurrentClientAuthority"
>;

/** Reconciliation reads do not consume a fresh creator capability. */
export async function inspectCronReplayForCaller(
  args: ReplayReadOptions,
  replay: CronReplayRequest,
) {
  const assertCurrent = () => {
    assertCronReadCurrent(args);
    const identity = args.client?.internal?.agentRuntimeIdentity;
    if (identity) {
      getCronManagementAuthority(identity)?.();
    }
  };
  const result = await args.context.cron.replay({ action: "inspect", replay }, assertCurrent);
  assertCurrent();
  if (result.state === "not-applied") {
    throw new TypeError("cron replay attempt is closed; reviewed retry required");
  }
  return result;
}

/** Receipt lookup precedes live job lookup: one-shot jobs may already be deleted. */
export function wrapCronRunReplay(handlers: GatewayRequestHandlers): void {
  for (const [method, handler] of Object.entries(handlers)) {
    if (method !== "cron.run") {
      continue;
    }
    handlers[method] = async (args) => {
      if (!isRecord(args.params) || args.params.replay === undefined) {
        await handler(args);
        return;
      }
      if (!assertValidParams(args.params, validateCronRunParams, method, args.respond)) {
        return;
      }
      const p = args.params;
      if (p.replay) {
        try {
          const jobId = resolveCronJobId(p);
          if (!jobId || p.mode === "if-enabled") {
            throw new TypeError("cron replay requires a job and due or force mode");
          }
          if (
            p.expectedProcessInstanceId &&
            p.expectedProcessInstanceId !== getGatewayProcessInstanceId()
          ) {
            throw new TypeError("Gateway process changed after preflight");
          }
          const replay = cronReplayRequest(args.client, p.replay, {
            action: "run",
            jobId,
            mode: p.mode === "due" ? "due" : "force",
          });
          if (!replay) {
            throw new TypeError("cron replay identity required");
          }
          const result = await inspectCronReplayForCaller(args, replay);
          if (result.state === "accepted") {
            if (!("enqueued" in result.result)) {
              throw new TypeError("cron replay result is not a run");
            }
            args.respond(
              true,
              { ...result.result, processInstanceId: getGatewayProcessInstanceId() },
              undefined,
            );
            return;
          }
        } catch (error) {
          respondInvalidCronParams(args.respond, method, formatErrorMessage(error));
          return;
        }
      }
      await handler(args);
    };
  }
}

async function control(args: GatewayRequestHandlerOptions, command: CronReplayControl) {
  const { client, context, respond, sessionMutationCommitGuard, hasCurrentClientAuthority } = args;
  // Runtime management grants must not let the model admit fresh IDs to evade a hold.
  if (
    client?.internal?.agentRuntimeIdentity ||
    !client?.connect.scopes?.includes("operator.admin")
  ) {
    respondInvalidCronParams(respond, "cron.replay", "trusted operator.admin admission required");
    return;
  }
  const guard = resolveCronMutationCommitGuard(client, context, undefined, {
    sessionMutationCommitGuard,
    hasCurrentClientAuthority,
  });
  const assertCurrent = () => {
    guard?.();
    if (hasCurrentClientAuthority?.() === false) {
      throw new TypeError("cron replay operator authority expired");
    }
  };
  try {
    assertCurrent();
    const result = await context.cron.replay(command, assertCurrent);
    if (command.action === "inspect") {
      assertCurrent();
    }
    if (result.state === "accepted" && !("enqueued" in result.result)) {
      respond(
        true,
        { state: "accepted", result: cronAddResultReadView({ result: result.result }) },
        undefined,
      );
    } else {
      respond(true, result, undefined);
    }
  } catch (error) {
    respondInvalidCronParams(respond, "cron.replay", formatErrorMessage(error));
  }
}

export const cronReplayHandlers: GatewayRequestHandlers = {
  "cron.replay.prepareAdd": async (args) => {
    try {
      const { candidate, enabledExplicit } = normalizeCronAddRequest(args.params);
      if (
        !assertValidParams(candidate, validateCronAddParams, "cron.replay.prepareAdd", args.respond)
      ) {
        return;
      }
      const { replay: _replay, ...createParams } = candidate;
      const input = applyCronCreateCallerScopeDefault(
        createParams as CronJobCreate,
        readCronCallerScope(args.client),
      );
      assertCronReadCurrent(args);
      args.respond(
        true,
        {
          authorityId: authorityId(args.client),
          intent: {
            action: "add",
            intentHash: cronReplayIntentHash({ input, enabledExplicit }),
          },
          input,
          enabledExplicit,
        },
        undefined,
      );
    } catch (error) {
      respondInvalidCronParams(args.respond, "cron.replay.prepareAdd", formatErrorMessage(error));
    }
  },
  "cron.replay.admit": async (args) => {
    if (
      !assertValidParams(
        args.params,
        validateCronReplayAdmitParams,
        "cron.replay.admit",
        args.respond,
      )
    ) {
      return;
    }
    const p = args.params;
    await control(args, {
      action: "admit",
      replay: {
        ref: { operationId: p.operationId, effectId: p.effectId, attempt: 1 },
        authorityId: p.authorityId,
        intent: p.intent,
      },
      evidence: p.evidence,
      maxAttempts: p.maxAttempts ?? 2,
    });
  },
  "cron.replay.inspect": async (args) => {
    if (
      !assertValidParams(
        args.params,
        validateCronReplayInspectParams,
        "cron.replay.inspect",
        args.respond,
      )
    ) {
      return;
    }
    const p = args.params;
    await control(args, {
      action: "inspect",
      replay: {
        ref: {
          operationId: p.operationId,
          effectId: p.effectId,
          attempt: p.attempt,
        },
        authorityId: p.authorityId,
        intent: p.intent,
      },
    });
  },
  "cron.replay.close": async (args) => {
    if (
      !assertValidParams(
        args.params,
        validateCronReplayCloseParams,
        "cron.replay.close",
        args.respond,
      )
    ) {
      return;
    }
    const p = args.params;
    await control(args, {
      action: "close",
      replay: {
        ref: {
          operationId: p.operationId,
          effectId: p.effectId,
          attempt: p.attempt,
        },
        authorityId: p.authorityId,
        intent: p.intent,
      },
    });
  },
  "cron.replay.retry": async (args) => {
    if (
      !assertValidParams(
        args.params,
        validateCronReplayRetryParams,
        "cron.replay.retry",
        args.respond,
      )
    ) {
      return;
    }
    const p = args.params;
    await control(args, {
      action: "retry",
      replay: {
        ref: {
          operationId: p.operationId,
          effectId: p.effectId,
          attempt: p.attempt,
        },
        authorityId: p.authorityId,
        intent: p.intent,
      },
      evidence: p.evidence,
    });
  },
};
