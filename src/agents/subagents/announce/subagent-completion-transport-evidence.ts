import type { OutboundDeliveryResult } from "../../../infra/outbound/deliver-types.js";

/** Diagnostic custody from the send owner, not authorization or transcript inference. */
export type CompletionTransportEvidence = {
  childRunId: string;
  childSessionKey: string;
  requesterSessionKey: string;
  channel: string;
  to: string;
  accountId?: string;
  threadId?: string;
  platformMessageIds: string[];
  scope: "partial" | "complete";
};

export function collectCompletionTransportEvidence(params: {
  childRunId?: string;
  childSessionKey: string;
  requesterSessionKey: string;
  target: { channel?: string; to?: string; accountId?: string; threadId?: string };
  results: readonly OutboundDeliveryResult[];
  complete: boolean;
}): CompletionTransportEvidence | undefined {
  if (!params.childRunId || !params.target.channel || !params.target.to || !params.results.length) {
    return undefined;
  }
  const ids: string[] = [];
  for (const result of params.results) {
    // Deliberately require an actual platform message ID (not a poll ID, queue ID or label).
    const id = result.messageId?.trim();
    if (result.outcome === "not_sent" || result.channel !== params.target.channel ||
        !id || id === "unknown" || id === "suppressed" ||
        (result.target && result.target.id !== params.target.to)) {
      return undefined;
    }
    if (!ids.includes(id)) ids.push(id);
  }
  return {
    childRunId: params.childRunId,
    childSessionKey: params.childSessionKey,
    requesterSessionKey: params.requesterSessionKey,
    channel: params.target.channel,
    to: params.target.to,
    ...(params.target.accountId ? { accountId: params.target.accountId } : {}),
    ...(params.target.threadId ? { threadId: params.target.threadId } : {}),
    platformMessageIds: ids,
    scope: params.complete ? "complete" : "partial",
  };
}
