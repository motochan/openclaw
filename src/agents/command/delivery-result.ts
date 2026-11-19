import { AsyncLocalStorage } from "node:async_hooks";
import type { SerializedDurableMessagePayloadOutcome } from "../../channels/message/runtime.js";
import type { projectOutboundPayloadPlanForJson } from "../../infra/outbound/payloads.js";
import { hasAnyNonEmptyString as hasNonEmptyStringArray } from "../delivery-evidence-values.js";
import type { MessagingToolSend } from "../embedded-agent-messaging.types.js";
import type {
  EmbeddedAgentRunMeta,
  EmbeddedAgentRunResult,
} from "../embedded-agent-runner/types.js";

/** Aggregate delivery status for an agent command result. */
export type AgentCommandDeliveryStatus = {
  requested: true;
  attempted: boolean;
  status: "sent" | "suppressed" | "partial_failed" | "failed";
  /** `partial` means at least one payload was sent before a later payload failed. */
  succeeded: true | false | "partial";
  error?: true;
  errorMessage?: string;
  /** Free-form lowercase_snake reason from durable delivery or preflight validation. */
  reason?: string;
  resultCount?: number;
  /** Positive actual identities; absent/empty does not prove loss. */
  platformMessageIds?: readonly string[];
  completionBatchTransport?: CompletionBatchMessageTransport;
  sentBeforeError?: true;
  payloadOutcomes?: SerializedDurableMessagePayloadOutcome[];
};

/** Agent command result after payload normalization and optional delivery. */
export type AgentCommandDeliveryResult = Pick<
  EmbeddedAgentRunResult,
  | "didDeliverSourceReplyViaMessageTool"
  | "sourceReplyDelivered"
  | "sourceReplyDeliveryState"
  | "messagingToolSourceReplyPayloads"
> & {
  payloads: ReturnType<typeof projectOutboundPayloadPlanForJson>;
  meta: EmbeddedAgentRunMeta;
  didSendViaMessagingTool?: boolean;
  messagingToolSentTexts?: string[];
  messagingToolSentMediaUrls?: string[];
  messagingToolSentTargets?: MessagingToolSend[];
  didSendDeterministicApprovalPrompt?: true;
  acceptedSessionSpawns?: NonNullable<EmbeddedAgentRunResult["acceptedSessionSpawns"]>;
  requesterContinuationSettled?: true;
  successfulCronAdds?: number;
  deliverySucceeded?: boolean;
  deliveryStatus?: AgentCommandDeliveryStatus;
};

function hasNonEmptyArray<T>(value: T[] | undefined): value is T[] {
  return Array.isArray(value) && value.length > 0;
}

export function buildDeliveryResult(params: {
  payloads: AgentCommandDeliveryResult["payloads"];
  meta: AgentCommandDeliveryResult["meta"];
  result: EmbeddedAgentRunResult;
  deliverySucceeded?: boolean;
  deliveryStatus?: AgentCommandDeliveryStatus;
}): AgentCommandDeliveryResult {
  const successfulCronAdds = params.result.successfulCronAdds;
  const hasSuccessfulCronAdds =
    typeof successfulCronAdds === "number" &&
    Number.isFinite(successfulCronAdds) &&
    successfulCronAdds > 0;
  return {
    payloads: params.payloads,
    meta: params.meta,
    ...(params.result.didSendViaMessagingTool === true ? { didSendViaMessagingTool: true } : {}),
    ...(params.result.didDeliverSourceReplyViaMessageTool === true
      ? { didDeliverSourceReplyViaMessageTool: true }
      : {}),
    ...(params.result.sourceReplyDelivered ? { sourceReplyDelivered: true as const } : {}),
    ...(params.result.sourceReplyDeliveryState !== undefined
      ? { sourceReplyDeliveryState: params.result.sourceReplyDeliveryState }
      : {}),
    ...(hasNonEmptyArray(params.result.messagingToolSourceReplyPayloads)
      ? { messagingToolSourceReplyPayloads: params.result.messagingToolSourceReplyPayloads }
      : {}),
    ...(hasNonEmptyStringArray(params.result.messagingToolSentTexts)
      ? { messagingToolSentTexts: params.result.messagingToolSentTexts }
      : {}),
    ...(hasNonEmptyStringArray(params.result.messagingToolSentMediaUrls)
      ? { messagingToolSentMediaUrls: params.result.messagingToolSentMediaUrls }
      : {}),
    ...(hasNonEmptyArray(params.result.messagingToolSentTargets)
      ? { messagingToolSentTargets: params.result.messagingToolSentTargets }
      : {}),
    ...(params.result.didSendDeterministicApprovalPrompt === true
      ? { didSendDeterministicApprovalPrompt: true }
      : {}),
    ...(hasNonEmptyArray(params.result.acceptedSessionSpawns)
      ? { acceptedSessionSpawns: params.result.acceptedSessionSpawns }
      : {}),
    ...(params.result.requesterContinuationSettled === true
      ? { requesterContinuationSettled: true as const }
      : {}),
    ...(hasSuccessfulCronAdds ? { successfulCronAdds } : {}),
    ...(params.deliverySucceeded !== undefined
      ? { deliverySucceeded: params.deliverySucceeded }
      : {}),
    ...(params.deliveryStatus ? { deliveryStatus: params.deliveryStatus } : {}),
  };
}

/** Parent-message transport only; never an exact child-report delivery receipt. */
export type CompletionBatchMessageTransport = {
  kind: "completion-batch-message";
  parentRunId: string;
  requesterSessionKey: string;
  requesterSessionId: string;
  admittedRunIds: readonly string[];
  representedRunIds: readonly string[];
  omittedRunIds: readonly string[];
  reportDelivery: "UNKNOWN";
  transportStatus: "complete" | "partial";
  platformMessageIds: readonly string[];
};

type CompletionBatchDeliveryContext = Omit<
  CompletionBatchMessageTransport,
  "kind" | "reportDelivery" | "transportStatus" | "platformMessageIds"
> & { isCurrent: () => boolean };

// Native lexical owner attribution, not permission or public request metadata.
// Existing admission and platform-send guards retain exclusive authority.
const completionBatchDeliveryContext = new AsyncLocalStorage<CompletionBatchDeliveryContext>();

export async function withCompletionBatchDeliveryContext<T>(
  context: CompletionBatchDeliveryContext,
  run: () => Promise<T>,
): Promise<T> {
  let active = true;
  const captured = {
    ...context,
    admittedRunIds: Object.freeze([...context.admittedRunIds]),
    representedRunIds: Object.freeze([...context.representedRunIds]),
    omittedRunIds: Object.freeze([...context.omittedRunIds]),
    isCurrent: () => active && context.isCurrent(),
  };
  try {
    return await completionBatchDeliveryContext.run(captured, run);
  } finally {
    active = false;
  }
}

export function readCompletionBatchMessageTransport(params: {
  parentRunId?: string;
  requesterSessionKey?: string;
  requesterSessionId?: string;
  status: AgentCommandDeliveryStatus;
}): CompletionBatchMessageTransport | undefined {
  const context = completionBatchDeliveryContext.getStore();
  try {
    if (
      !context ||
      context.parentRunId !== params.parentRunId ||
      context.requesterSessionKey !== params.requesterSessionKey ||
      context.requesterSessionId !== params.requesterSessionId ||
      !context.isCurrent() ||
      (params.status.status !== "sent" && params.status.status !== "partial_failed") ||
      !params.status.platformMessageIds?.length ||
      context.representedRunIds.some((id) => !context.admittedRunIds.includes(id))
    ) {
      return undefined;
    }
    return {
      kind: "completion-batch-message",
      parentRunId: context.parentRunId,
      requesterSessionKey: context.requesterSessionKey,
      requesterSessionId: context.requesterSessionId,
      admittedRunIds: context.admittedRunIds,
      representedRunIds: context.representedRunIds,
      omittedRunIds: context.omittedRunIds,
      reportDelivery: "UNKNOWN",
      transportStatus: params.status.status === "sent" ? "complete" : "partial",
      platformMessageIds: params.status.platformMessageIds,
    };
  } catch {
    // Optional attribution failures cannot abort delivery or invent healthy evidence.
    return undefined;
  }
}
