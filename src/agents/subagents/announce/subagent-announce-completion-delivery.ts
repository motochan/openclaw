import { createHash } from "node:crypto";
import { asOptionalObjectRecord, isRecord } from "@openclaw/normalization-core/record-coerce";
import { truncateUtf16Safe } from "@openclaw/normalization-core/utf16-slice";
import { sanitizePendingFinalDeliveryText } from "../../../auto-reply/reply/pending-final-delivery-state.js";
import type { MessageReceipt } from "../../../channels/message/types.js";
import { isChannelPartialDeliveryError } from "../../../channels/turn/partial-delivery-error.js";
import {
  getRestartRecoveryTerminalDeliveryEvidence,
  hasRestartRecoverySourceClaim,
  hasRestartRecoveryTerminalRun,
} from "../../../config/sessions/restart-recovery-state.js";
import type { RestartRecoveryTerminalDeliveryEvidence } from "../../../config/sessions/restart-recovery-types.js";
import type { SessionEntry } from "../../../config/sessions/types.js";
import type { OpenClawConfig } from "../../../config/types.openclaw.js";
import { waitForGatewayDispatch } from "../../../gateway/server-in-process-dispatch.js";
import type { GatewayRecoveryTypingParams } from "../../../gateway/server-instance-runtime.types.js";
import { getGatewayRecoveryRuntime } from "../../../gateway/server-recovery-runtime-context.js";
import { isOutboundDeliveryError } from "../../../infra/outbound/deliver-types.js";
import { normalizeOutboundReplyPayloadCore } from "../../../infra/outbound/reply-payload-normalize.js";
import { sourceDeliveryTargetsMatch } from "../../../infra/outbound/source-delivery-plan.js";
import { splitMediaFromOutput } from "../../../media/parse.js";
import { shouldPreserveUserFacingSessionStateForInputProvenance } from "../../../sessions/input-provenance.js";
import { isNonTerminalAgentRunStatus } from "../../../shared/agent-run-status.js";
import type { DeliveryContext } from "../../../utils/delivery-context.types.js";
import { buildAgentRunTerminalOutcomeFromWaitResult } from "../../agent-run-terminal-outcome.js";
import { sanitizeAgentRunTerminalReplyText } from "../../agent-run-terminal-reply.js";
import {
  getGatewayAgentResult,
  hasCommittedSourceReplyDeliveryEvidence,
  hasMessagingToolDeliveryEvidence,
  hasUnaccountedMessagingToolAggregateEvidence,
  resolveExplicitFinalSourceReplyDeliveryEvidence,
} from "../../embedded-agent-runner/delivery-evidence.js";
import { hasVisibleAgentPayload } from "../../embedded-agent-runner/message-visibility.js";
import { hasVisibleCompletionResult } from "../../internal-event-contract.js";
import { collectAgentInternalEventMedia, type AgentInternalEvent } from "../../internal-events.js";
import { createAgentRunDirectAbortError } from "../../run-termination.js";
import {
  hasAnnounceSendEvidence,
  SourceOwnerChangedError,
  summarizeDeliveryError,
} from "./subagent-announce-delivery-retry.js";
import {
  sendSubagentAnnounceMessage,
  tryResolveSubagentRequesterAgentId,
} from "./subagent-announce-delivery.runtime.js";
import {
  sourceOwnerChangedResult,
  type SubagentAnnounceDeliveryResult,
} from "./subagent-announce-dispatch.js";
import type { SubagentCompletionToolHandoffRegistration } from "./subagent-announce-handoff.js";
import { isDirectMessageDeliveryTarget } from "./subagent-announce-origin.js";
import type {
  PreparedAnnounceResult,
  PreparedCompleteSubagentResult,
} from "./subagent-announce-result.js";
import { dispatchGatewayMethodInProcess } from "./subagent-announce.runtime.js";

export { isDirectMessageDeliveryTarget } from "./subagent-announce-origin.js";

export async function runAnnounceAgentCall(params: {
  agentParams: Record<string, unknown>;
  privateCompletion?: true;
  typing?: Omit<GatewayRecoveryTypingParams, "isCurrent">;
  settleWakeSourceSessionKeys?: readonly string[];
  delegatedToolPolicyHandoff?: SubagentCompletionToolHandoffRegistration;
  expectFinal?: boolean;
  onAccepted?: (payload: unknown) => void;
  onExecutionStarted?: () => void;
  signal?: AbortSignal;
  timeoutMs?: number;
  isExecutionAllowed: () => boolean;
  isSourceSessionAdmissionAllowed?: () => boolean;
  resolveGatewayContext?: import("../../../gateway/server-methods/types.js").GatewayContextResolver;
}): Promise<unknown> {
  const typingRuntime = params.typing
    ? params.resolveGatewayContext
      ? params.resolveGatewayContext()?.recoveryRuntime
      : getGatewayRecoveryRuntime()
    : undefined;
  let stopTyping: (() => void) | undefined;
  const deadline = new AbortController();
  const sourceLifecycle = new AbortController();
  const isSourceSessionAdmissionAllowed = params.isSourceSessionAdmissionAllowed;
  const lifecycleSignal = params.signal
    ? AbortSignal.any([params.signal, sourceLifecycle.signal])
    : sourceLifecycle.signal;
  const signal = AbortSignal.any([lifecycleSignal, deadline.signal]);
  // A private input stays owned by Gateway admission when an observer times out.
  // Caller or source lifecycle cancellation still stops that underlying turn.
  const executionSignal = params.privateCompletion ? lifecycleSignal : signal;
  const timer =
    params.timeoutMs === undefined
      ? undefined
      : setTimeout(
          () => deadline.abort(new Error("gateway request timeout for agent")),
          params.timeoutMs,
        );
  timer?.unref?.();
  try {
    signal.throwIfAborted();
    const dispatch = dispatchGatewayMethodInProcess("agent", params.agentParams, {
      cancelOnDeadline: true,
      privateCompletion: params.privateCompletion,
      settleWakeReplay: params.settleWakeSourceSessionKeys
        ? {
            sourceSessionKeys: params.settleWakeSourceSessionKeys,
            assertCurrent: () => {
              if (!params.isExecutionAllowed()) {
                throw new SourceOwnerChangedError();
              }
            },
          }
        : undefined,
      expectFinal: params.expectFinal,
      forceSyntheticClient: shouldPreserveUserFacingSessionStateForInputProvenance(
        params.agentParams.inputProvenance,
      ),
      operatorRoleActor: { kind: "system" },
      delegatedToolPolicyHandoff: params.delegatedToolPolicyHandoff,
      signal: executionSignal,
      ...(isSourceSessionAdmissionAllowed
        ? {
            sessionMutationCommitGuard: () => {
              if (!isSourceSessionAdmissionAllowed()) {
                const error = new SourceOwnerChangedError();
                sourceLifecycle.abort(error);
                throw error;
              }
            },
          }
        : {}),
      // Accepted queue waits belong to session admission; execution belongs to
      // the requester runtime budget, not the announcement handoff deadline.
      onAccepted: (payload) => {
        clearTimeout(timer);
        params.onAccepted?.(payload);
      },
      onExecutionStarted: () => {
        executionSignal.throwIfAborted();
        if (!params.isExecutionAllowed()) {
          sourceLifecycle.abort(new SourceOwnerChangedError());
          // Classify execution immediately, before Gateway observes cancellation.
          throw createAgentRunDirectAbortError();
        }
        // Execution can be observed before acceptance on an already-running replay.
        clearTimeout(timer);
        params.onExecutionStarted?.();
        if (params.typing) {
          stopTyping ??= typingRuntime?.startRecoveryTyping?.({
            ...params.typing,
            isCurrent: () =>
              !executionSignal.aborted &&
              params.isExecutionAllowed() &&
              (params.resolveGatewayContext
                ? params.resolveGatewayContext()?.recoveryRuntime === typingRuntime
                : getGatewayRecoveryRuntime() === typingRuntime),
          });
        }
      },
      resolveGatewayContext: params.resolveGatewayContext,
    });
    return params.privateCompletion
      ? await waitForGatewayDispatch("agent", dispatch, undefined, signal)
      : await dispatch;
  } catch (error) {
    sourceLifecycle.signal.throwIfAborted();
    throw error;
  } finally {
    clearTimeout(timer);
    stopTyping?.();
  }
}

const FAILED_COMPLETION_NOTICE =
  "A delegated task failed before it could report a result. Please retry the task.";

export function isGatewayAgentRunPending(response: unknown): boolean {
  return isNonTerminalAgentRunStatus(asOptionalObjectRecord(response)?.status);
}

/** A recovery successor owns its admitted input until its exact final can be reconciled. */
export function resolveRequesterRecoveryDelivery(
  entry: SessionEntry | undefined,
  runId: string,
):
  | { kind: "result"; result: RestartRecoveryTerminalDeliveryEvidence }
  | { kind: "delivery"; delivery: SubagentAnnounceDeliveryResult }
  | undefined {
  const result = getRestartRecoveryTerminalDeliveryEvidence(entry, runId);
  if (result) {
    return { kind: "result", result };
  }
  if (hasRestartRecoverySourceClaim(entry, runId)) {
    return {
      kind: "delivery",
      delivery: {
        delivered: false,
        path: "direct",
        reason: "requester_turn_pending",
        disposition: "retryable",
      },
    };
  }
  if (hasRestartRecoveryTerminalRun(entry, runId)) {
    return {
      kind: "delivery",
      delivery: {
        delivered: false,
        path: "direct",
        reason: "visible_reply_missing",
        error: "recovered requester completed without durable final delivery evidence",
        disposition: "permanent_failure",
      },
    };
  }
  return undefined;
}

export function resolvePrivateCompletionDeliveryResult(
  response: Record<string, unknown> | undefined,
  origin?: DeliveryContext,
): SubagentAnnounceDeliveryResult {
  const outcome = buildAgentRunTerminalOutcomeFromWaitResult(response);
  if (outcome?.reason === "cancelled" && outcome.stopReason !== "restart") {
    return {
      delivered: false,
      path: "direct",
      terminal: true,
      reason: "delivery_suppressed",
      disposition: "intentional_non_delivery",
      error: "private requester continuation was cancelled",
    };
  }
  // Successful internal consumption may be silent or start the next child.
  // Queue acceptance alone is not consumption, and no external receipt is owed.
  const delivery: SubagentAnnounceDeliveryResult =
    response?.status === "ok" && response?.inputProcessingCompleted === true
      ? { delivered: true, path: "direct" }
      : {
          delivered: false,
          path: "direct",
          reason: "completion_handoff_pending",
          error: "private requester turn has not completed successfully",
          disposition: "retryable",
        };
  const result = getGatewayAgentResult(response);
  if (
    delivery.delivered &&
    origin?.channel &&
    origin.to &&
    result &&
    result.meta?.yielded !== true &&
    result.meta?.continuationPending !== true &&
    hasMessagingToolDeliveryToSource(result, origin, { requireFinalReply: true })
  ) {
    delivery.requesterVisibleFinalDelivered = true;
  }
  return delivery;
}

export function buildRequesterCompletionDeliveryResult(
  finalCommitted: boolean,
  text: unknown,
): SubagentAnnounceDeliveryResult {
  const finalAssistantVisibleText =
    finalCommitted && typeof text === "string" ? truncateUtf16Safe(text.trim(), 12_000) : "";
  return {
    delivered: true,
    path: "direct",
    // A canceled partial payload or accepted handoff is not a visible final receipt.
    ...(finalCommitted ? { requesterVisibleFinalDelivered: true } : {}),
    ...(finalAssistantVisibleText ? { finalAssistantVisibleText } : {}),
  };
}

type DirectCompletionContent = {
  content: string;
  mediaUrls: string[];
  audioAsVoice?: boolean;
  completeResult?: PreparedCompleteSubagentResult;
};

function matchesCompleteResultText(text: string, result: PreparedCompleteSubagentResult): boolean {
  return (
    Buffer.byteLength(text, "utf8") === result.utf8ByteLength &&
    createHash("sha256").update(text, "utf8").digest("hex") === result.resultHash
  );
}

function collectDirectCompletionContent(params: {
  agentResult?: { payloads?: unknown };
  events: readonly AgentInternalEvent[] | undefined;
  contentKind: "completed_result" | "failed_notice";
  preparedResult?: PreparedAnnounceResult;
  sourceRunId?: string;
  sourceSessionKey?: string;
  requesterSessionKey: string;
}): DirectCompletionContent | undefined {
  if (params.contentKind === "failed_notice") {
    return { content: FAILED_COMPLETION_NOTICE, mediaUrls: [] };
  }
  const collect = (
    payloads: readonly unknown[],
    fullText = false,
  ): DirectCompletionContent | undefined => {
    const textParts: string[] = [];
    const mediaUrls = new Set<string>();
    let audioAsVoice = false;
    for (const record of payloads) {
      if (!isRecord(record)) {
        continue;
      }
      if (
        !hasVisibleAgentPayload(
          { payloads: [record] },
          {
            includeErrorPayloads: false,
            includeReasoningPayloads: false,
            includeSilentReplyPayloads: false,
            requireTerminalContent: true,
          },
        )
      ) {
        continue;
      }
      const normalized = normalizeOutboundReplyPayloadCore(record);
      // Hidden runtime context must not contribute media directives: strip the
      // protected block before extraction so a MEDIA reference it carries can
      // never become an attachment; visible directives still deliver.
      const parsed = splitMediaFromOutput(sanitizePendingFinalDeliveryText(normalized.text ?? ""));
      if (parsed.audioAsVoice === true || record.audioAsVoice === true) {
        audioAsVoice = true;
      }
      const safeText = sanitizePendingFinalDeliveryText(parsed.text);
      const text = fullText ? safeText : sanitizeAgentRunTerminalReplyText(safeText);
      // A result that only reads like the producer's placeholder is still a real
      // result: absence is recorded on the event fact, never matched here.
      if (text) {
        textParts.push(text);
      }
      for (const mediaUrl of [
        ...(normalized.mediaUrl ? [normalized.mediaUrl] : []),
        ...(normalized.mediaUrls ?? []),
        ...(parsed.mediaUrls ?? []),
      ]) {
        mediaUrls.add(mediaUrl);
      }
    }
    return textParts.length > 0 || mediaUrls.size > 0
      ? {
          content: textParts.join("\n\n"),
          mediaUrls: [...mediaUrls],
          ...(audioAsVoice ? { audioAsVoice: true as const } : {}),
        }
      : undefined;
  };

  const payloadContent = Array.isArray(params.agentResult?.payloads)
    ? collect(params.agentResult.payloads)
    : undefined;
  if (payloadContent && payloadContent.mediaUrls.length > 0) {
    return payloadContent;
  }
  for (let index = (params.events?.length ?? 0) - 1; index >= 0; index -= 1) {
    const event = params.events?.[index];
    if (event?.type !== "task_completion" || event.source !== "subagent" || event.status !== "ok") {
      continue;
    }
    // Placeholder copy for an absent child result is not deliverable content.
    if (!hasVisibleCompletionResult(event)) {
      continue;
    }
    const prepared = params.preparedResult;
    const identity = prepared?.completeResult;
    const exactResult =
      identity &&
      prepared?.isCurrent() &&
      identity.runId === params.sourceRunId &&
      identity.childSessionKey === params.sourceSessionKey &&
      identity.childSessionKey === event.childSessionKey &&
      identity.requesterSessionKey === params.requesterSessionKey &&
      prepared.text === event.result &&
      matchesCompleteResultText(event.result, identity)
        ? identity
        : undefined;
    // Only exact retained owner text bypasses the bounded lifecycle excerpt.
    // All safety sanitation and ordinary outbound splitting remain in force.
    const parsedEvent = collect([{ text: event.result }], exactResult !== undefined);
    const eventMediaUrls = collectAgentInternalEventMedia([event]).mediaUrls;
    const mediaUrls = new Set([...(parsedEvent?.mediaUrls ?? []), ...eventMediaUrls]);
    if (parsedEvent || mediaUrls.size > 0) {
      return {
        content: parsedEvent?.content ?? "",
        mediaUrls: [...mediaUrls],
        ...(parsedEvent?.audioAsVoice ? { audioAsVoice: true as const } : {}),
        ...(exactResult &&
        parsedEvent &&
        mediaUrls.size === 0 &&
        !parsedEvent.audioAsVoice &&
        matchesCompleteResultText(parsedEvent.content, exactResult)
          ? { completeResult: exactResult }
          : {}),
      };
    }
  }
  return undefined;
}

export async function deliverCompletionDirect(params: {
  cfg: OpenClawConfig;
  requesterSessionKey: string;
  requesterAgentId?: string;
  directIdempotencyKey: string;
  deliveryTarget: {
    deliver: boolean;
    channel?: string;
    to?: string;
    accountId?: string;
    threadId?: string;
  };
  internalEvents?: readonly AgentInternalEvent[];
  /** Private retained-result owner; never reconstructed from a prompt or receipt export. */
  preparedResult?: PreparedAnnounceResult;
  sourceRunId?: string;
  sourceSessionKey?: string;
  contentKind: "completed_result" | "failed_notice";
  signal?: AbortSignal;
  agentResult?: { payloads?: unknown };
  onDeliveryResult?: (delivery: SubagentAnnounceDeliveryResult) => void | Promise<void>;
  isSourceSessionEffectsAllowed?: () => boolean;
}): Promise<SubagentAnnounceDeliveryResult | undefined> {
  const completionContent = collectDirectCompletionContent({
    agentResult: params.agentResult,
    events: params.internalEvents,
    contentKind: params.contentKind,
    // Other transports retain their existing bounded behavior and UNKNOWN membership.
    preparedResult:
      params.deliveryTarget.channel === "telegram" ? params.preparedResult : undefined,
    sourceRunId: params.sourceRunId,
    sourceSessionKey: params.sourceSessionKey,
    requesterSessionKey: params.requesterSessionKey,
  });
  // A failed completion must not deliver partial child media as its result.
  const content = completionContent?.content;
  const mediaUrls = completionContent?.mediaUrls ?? [];
  const audioAsVoice = completionContent?.audioAsVoice === true;
  if (
    (!content && mediaUrls.length === 0) ||
    !params.deliveryTarget.deliver ||
    !params.deliveryTarget.channel ||
    !params.deliveryTarget.to ||
    !isDirectMessageDeliveryTarget(params.deliveryTarget, params.requesterSessionKey)
  ) {
    return undefined;
  }
  const agentId = tryResolveSubagentRequesterAgentId(
    params.cfg,
    params.requesterSessionKey,
    params.requesterAgentId,
  );
  if (!agentId) {
    return undefined;
  }
  const idempotencyKey = `${params.directIdempotencyKey}:text-direct`;
  let committedDelivery: SubagentAnnounceDeliveryResult | undefined;
  let deliveryResultReported: Promise<void> | undefined;
  const acceptedParts = new Map<
    string,
    NonNullable<SubagentAnnounceDeliveryResult["acceptedResultParts"]>[number]
  >();
  let acceptanceUnknown = false;
  const retainReceipt = (receipt: MessageReceipt | undefined) => {
    if (
      !receipt?.parts.length ||
      receipt.platformMessageIds.some(
        (id) => !receipt.parts.some((part) => part.platformMessageId === id),
      )
    ) {
      acceptanceUnknown = true;
      return;
    }
    for (const part of receipt.parts) {
      const acceptance = part.acceptance;
      if (
        !acceptance ||
        acceptance.channel !== "telegram" ||
        acceptance.accountId !== params.deliveryTarget.accountId ||
        acceptance.to !== params.deliveryTarget.to ||
        acceptance.threadId !== (params.deliveryTarget.threadId ?? null) ||
        !/^[1-9][0-9]*$/.test(part.platformMessageId) ||
        !Number.isFinite(acceptance.acceptedAtMs) ||
        acceptance.acceptedAtMs <= 0 ||
        acceptance.acceptedAtMs > Date.now()
      ) {
        acceptanceUnknown = true;
        continue;
      }
      const previous = acceptedParts.get(part.platformMessageId);
      if (previous) {
        if (previous.acceptance.acceptedAtMs !== acceptance.acceptedAtMs) {
          acceptanceUnknown = true;
        }
        continue;
      }
      acceptedParts.set(
        part.platformMessageId,
        Object.freeze({ platformMessageId: part.platformMessageId, acceptance }),
      );
    }
  };
  const snapshotAcceptedParts = () => Object.freeze([...acceptedParts.values()]);

  const isCurrent = () =>
    params.isSourceSessionEffectsAllowed?.() !== false &&
    (params.preparedResult?.isCurrent() ?? true);
  const assertDeliveryCurrent = () => {
    params.signal?.throwIfAborted();
    if (!isCurrent()) {
      throw new SourceOwnerChangedError();
    }
  };
  try {
    if (!isCurrent()) {
      return sourceOwnerChangedResult();
    }
    if (params.signal?.aborted) {
      return { delivered: false, path: "none" };
    }
    const sendResult = await sendSubagentAnnounceMessage({
      cfg: params.cfg,
      channel: params.deliveryTarget.channel,
      to: params.deliveryTarget.to,
      accountId: params.deliveryTarget.accountId,
      threadId: params.deliveryTarget.threadId,
      requesterSessionKey: params.requesterSessionKey,
      agentId,
      conversationType: "direct",
      content: content ?? "",
      ...(mediaUrls.length > 0 ? { mediaUrls } : {}),
      ...(audioAsVoice ? { asVoice: true } : {}),
      idempotencyKey,
      skipQueue: true,
      abortSignal: params.signal,
      onPlatformSendDispatch: async () => assertDeliveryCurrent(),
      assertDirectAdapterHandoff: assertDeliveryCurrent,
      onDeliveryResult: (result) => retainReceipt(result.receipt),
      onDeliveredPayload: (payload, results = []) => {
        if (committedDelivery) {
          return;
        }
        // This single payload must finish every chunk and attachment before settling,
        // still ahead of potentially blocked transcript mirroring.
        const identity = completionContent?.completeResult;
        // The final adapter receipts, not progress callbacks or original text,
        // own physical plan coverage. Missing coverage remains UNKNOWN.
        const completeCoverage =
          results.length > 0 &&
          results.every((result) => result.receipt?.textCoverage === "complete");
        const incompleteCoverage = results.some(
          (result) => result.receipt?.textCoverage === "partial",
        );
        const completePresentation =
          identity &&
          isCurrent() &&
          payload.mediaUrls.length === 0 &&
          !payload.audioAsVoice &&
          !payload.presentation &&
          !payload.interactive &&
          !payload.location &&
          matchesCompleteResultText(payload.text, identity);
        committedDelivery = {
          delivered: !incompleteCoverage,
          path: "direct",
          deliveredAt: Date.now(),
          ...(incompleteCoverage
            ? {
                terminal: true,
                disposition: "permanent_failure" as const,
                error:
                  "required text plan was only partially accepted; automatic retry would duplicate sent chunks",
              }
            : {}),
          // Effective post-hook payload membership only, NOT a physical receipt.
          ...(completePresentation ? { completeResultPresentation: identity } : {}),
          ...(acceptedParts.size ? { acceptedResultParts: snapshotAcceptedParts() } : {}),
          ...(completePresentation &&
          completeCoverage &&
          !acceptanceUnknown &&
          acceptedParts.size > 0
            ? {
                completeResultReceipt: Object.freeze({
                  result: identity,
                  platformMessageIds: Object.freeze([...acceptedParts.keys()]),
                  acceptedAtMs: Math.max(
                    ...[...acceptedParts.values()].map((part) => part.acceptance.acceptedAtMs),
                  ),
                  scope: "complete" as const,
                }),
              }
            : {}),
        };
        deliveryResultReported = Promise.resolve(
          params.onDeliveryResult?.(committedDelivery),
        ).catch(() => {
          // Bookkeeping failure cannot make a fully sent result retryable.
        });
      },
      mirror: {
        sessionKey: params.requesterSessionKey,
        agentId,
        idempotencyKey,
      },
    });
    if (committedDelivery) {
      return committedDelivery;
    }
    if (sendResult.deliveryStatus === "suppressed") {
      const ambiguous = sendResult.suppressionReason === "adapter_returned_no_identity";
      return {
        delivered: false,
        path: "direct",
        reason: ambiguous ? undefined : "delivery_suppressed",
        error: ambiguous
          ? "text completion direct delivery could not be confirmed: adapter returned no identity"
          : `text completion direct delivery was suppressed: ${sendResult.suppressionReason ?? "unknown reason"}`,
        ...(ambiguous
          ? { disposition: "ambiguous" as const }
          : { disposition: "intentional_non_delivery" as const, terminal: true }),
      };
    }
    return { delivered: true, path: "direct" };
  } catch (err) {
    if (committedDelivery) {
      // Post-send bookkeeping must never turn an identified delivery into a
      // retryable failure and send the same completion twice.
      return committedDelivery;
    }
    if (hasAnnounceSendEvidence(err)) {
      const seen = new Set<unknown>();
      let current: unknown = err;
      // Follow only native typed delivery wrappers, with cycle/depth bounds.
      for (let depth = 0; depth < 8 && !seen.has(current); depth++) {
        seen.add(current);
        if (isChannelPartialDeliveryError(current)) {
          retainReceipt(current.deliveryResult.receipt);
          current = current.cause;
        } else if (isOutboundDeliveryError(current)) {
          for (const result of current.results) {
            retainReceipt(result.receipt);
          }
          current = current.cause;
        } else {
          break;
        }
      }
      return {
        ...(acceptedParts.size ? { acceptedResultParts: snapshotAcceptedParts() } : {}),
        delivered: false,
        path: "direct",
        terminal: true,
        disposition: "permanent_failure",
        error: `text completion direct delivery was incomplete; automatic retry would duplicate sent chunks: ${summarizeDeliveryError(err)}`,
        ...(mediaUrls.length > 0 ? { missingMediaUrls: mediaUrls } : {}),
      };
    }
    if (err instanceof SourceOwnerChangedError) {
      return sourceOwnerChangedResult();
    }
    if (params.signal?.aborted) {
      return { delivered: false, path: "none" };
    }
    return {
      delivered: false,
      path: "direct",
      error: `text completion direct delivery failed: ${summarizeDeliveryError(err)}`,
    };
  } finally {
    await deliveryResultReported;
  }
}

export function hasMessagingToolDeliveryToSource(
  result: {
    didDeliverSourceReplyViaMessageTool?: unknown;
    didSendViaMessagingTool?: unknown;
    messagingToolSentTargets?: unknown;
    messagingToolSourceReplyPayloads?: unknown;
  },
  deliveryTarget: Parameters<typeof sourceDeliveryTargetsMatch>[1],
  options?: { requireFinalReply?: boolean },
): boolean {
  const targets = Array.isArray(result.messagingToolSentTargets)
    ? result.messagingToolSentTargets
    : [];
  const sourceTargets = targets.filter((target) => {
    if (
      !target ||
      typeof target !== "object" ||
      Array.isArray(target) ||
      !deliveryTarget.channel ||
      !deliveryTarget.to
    ) {
      return false;
    }
    const record = target as Parameters<typeof sourceDeliveryTargetsMatch>[0];
    // Older source receipts omit `to`; explicit off-target sends must never satisfy it.
    const sourceTarget =
      typeof record.to === "string" && record.to.trim()
        ? record
        : { ...record, to: deliveryTarget.to };
    return sourceDeliveryTargetsMatch(sourceTarget, deliveryTarget);
  });
  if (options?.requireFinalReply) {
    const hasCommittedSourceDelivery =
      hasCommittedSourceReplyDeliveryEvidence(result) ||
      (hasMessagingToolDeliveryEvidence(result) && sourceTargets.length > 0);
    // Only current-source final markers count; another target's final cannot
    // turn a source progress update into the owed requester reply.
    return (
      hasCommittedSourceDelivery &&
      resolveExplicitFinalSourceReplyDeliveryEvidence({
        messagingToolSentTargets: sourceTargets,
        messagingToolSourceReplyPayloads: result.messagingToolSourceReplyPayloads,
      }) !== false
    );
  }
  if (
    hasCommittedSourceReplyDeliveryEvidence(result) ||
    hasUnaccountedMessagingToolAggregateEvidence({ ...result, didSendViaMessagingTool: false })
  ) {
    return true;
  }

  if (targets.length === 0 || !deliveryTarget.channel || !deliveryTarget.to) {
    return hasMessagingToolDeliveryEvidence(result);
  }

  return hasMessagingToolDeliveryEvidence(result) && sourceTargets.length > 0;
}
