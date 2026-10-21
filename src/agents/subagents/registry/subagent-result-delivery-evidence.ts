import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import type { SubagentAnnounceDeliveryResult } from "../announce/subagent-announce-dispatch.js";
import type { SubagentResultDeliveryEvidence } from "./subagent-registry-read.types.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";
import { getSubagentRunIdentity } from "./subagent-run-generation.js";

/** Allowlist committed metadata; old, corrupt and differently-owned rows stay UNKNOWN. */
export function normalizeSubagentResultDelivery(
  entry: SubagentRunRecord,
  value: unknown,
): SubagentResultDeliveryEvidence | undefined {
  const identity = getSubagentRunIdentity(entry);
  const generation = entry.delivery?.generation ?? 0;
  const origin = entry.delivery?.payload?.requesterOrigin ?? entry.requesterOrigin;
  if (
    !isRecord(value) ||
    value.runIdentity !== identity ||
    value.deliveryGeneration !== generation ||
    origin?.channel !== "telegram" ||
    !origin.to ||
    !origin.accountId ||
    !Array.isArray(value.acceptedParts) ||
    value.acceptedParts.length === 0 ||
    value.acceptedParts.length > 4_096
  )
    return undefined;
  const parts: Array<SubagentResultDeliveryEvidence["acceptedParts"][number]> = [];
  const ids = new Set<string>();
  for (const part of value.acceptedParts) {
    if (!isRecord(part) || !isRecord(part.acceptance)) return undefined;
    const accepted = part.acceptance;
    if (
      typeof part.platformMessageId !== "string" ||
      !/^[1-9][0-9]*$/.test(part.platformMessageId) ||
      ids.has(part.platformMessageId) ||
      accepted.channel !== "telegram" ||
      accepted.accountId !== origin.accountId ||
      accepted.to !== origin.to ||
      accepted.threadId !== (origin.threadId == null ? null : String(origin.threadId)) ||
      typeof accepted.acceptedAtMs !== "number" ||
      !Number.isFinite(accepted.acceptedAtMs) ||
      accepted.acceptedAtMs <= 0
    )
      return undefined;
    ids.add(part.platformMessageId);
    parts.push({
      platformMessageId: part.platformMessageId,
      acceptance: {
        channel: "telegram",
        accountId: origin.accountId,
        to: origin.to,
        threadId: origin.threadId == null ? null : String(origin.threadId),
        acceptedAtMs: accepted.acceptedAtMs,
      },
    });
  }
  const evidence: SubagentResultDeliveryEvidence = {
    runIdentity: identity,
    deliveryGeneration: generation,
    acceptedParts: parts,
  };
  const receipt = value.completeReceipt;
  if (receipt === undefined) return evidence;
  if (!isRecord(receipt) || !isRecord(receipt.result)) return evidence;
  const result = receipt.result;
  if (
    receipt.scope !== "complete" ||
    result.runIdentity !== identity ||
    result.runId !== entry.runId ||
    result.childSessionKey !== entry.childSessionKey ||
    result.requesterSessionKey !== entry.requesterSessionKey ||
    typeof result.resultHash !== "string" ||
    !/^[a-f0-9]{64}$/.test(result.resultHash) ||
    typeof result.utf8ByteLength !== "number" ||
    !Number.isSafeInteger(result.utf8ByteLength) ||
    result.utf8ByteLength <= 0 ||
    !Array.isArray(receipt.platformMessageIds) ||
    !isDeepStrictEqual(receipt.platformMessageIds, [...ids]) ||
    receipt.acceptedAtMs !== Math.max(...parts.map((part) => part.acceptance.acceptedAtMs))
  )
    return evidence;
  evidence.completeReceipt = {
    result: {
      runId: entry.runId,
      childSessionKey: entry.childSessionKey,
      requesterSessionKey: entry.requesterSessionKey,
      runIdentity: identity,
      resultHash: result.resultHash,
      utf8ByteLength: result.utf8ByteLength,
    },
    platformMessageIds: [...ids],
    acceptedAtMs: receipt.acceptedAtMs as number,
    scope: "complete",
  };
  return evidence;
}

/** Called only inside the existing generation/CAS-guarded lifecycle mutation. */
export function retainSubagentResultDelivery(
  entry: SubagentRunRecord,
  delivery: SubagentAnnounceDeliveryResult,
  expected: { runIdentity: string; deliveryGeneration: number },
): boolean {
  if (
    !entry.delivery ||
    getSubagentRunIdentity(entry) !== expected.runIdentity ||
    (entry.delivery.generation ?? 0) !== expected.deliveryGeneration ||
    delivery.path !== "direct"
  )
    return false;
  const complete =
    delivery.delivered &&
    delivery.completeResultReceipt &&
    isDeepStrictEqual(delivery.completeResultPresentation, delivery.completeResultReceipt.result)
      ? delivery.completeResultReceipt
      : undefined;
  const next = normalizeSubagentResultDelivery(entry, {
    ...expected,
    acceptedParts: delivery.acceptedResultParts,
    completeReceipt: complete,
  });
  if (!next) return false;
  const prior = entry.delivery.resultDelivery;
  if (prior) {
    if (
      prior.runIdentity !== next.runIdentity ||
      prior.deliveryGeneration !== next.deliveryGeneration
    )
      return false;
    if (prior.completeReceipt) return false; // A replay or conflicting receipt cannot replace settled facts.
    for (const part of prior.acceptedParts) {
      const incoming = next.acceptedParts.find(
        (item) => item.platformMessageId === part.platformMessageId,
      );
      if (!incoming || !isDeepStrictEqual(incoming, part)) return false;
    }
    if (isDeepStrictEqual(prior, next)) return false;
  }
  entry.delivery.resultDelivery = next;
  return true;
}

/** Bounded metadata-only view for the native list owner, after its scope check. */
export function projectSubagentResultDelivery(entry: SubagentRunRecord) {
  const base = {
    runId: entry.runId,
    childSessionKey: entry.childSessionKey,
    requesterSessionKey: entry.requesterSessionKey,
    ownerGeneration: entry.generation ?? 0,
    ownerCreatedAtMs: entry.createdAt,
    ownerRef: createHash("sha256").update(getSubagentRunIdentity(entry)).digest("hex"),
    deliveryGeneration: entry.delivery?.generation ?? 0,
  };
  const evidence = normalizeSubagentResultDelivery(entry, entry.delivery?.resultDelivery);
  if (!evidence)
    return {
      ...base,
      status: "UNKNOWN",
      reportDelivery: "UNKNOWN",
      reason: "no_valid_committed_receipt",
    };
  if (
    evidence.acceptedParts.length > 128 ||
    evidence.acceptedParts.some(
      (part) =>
        part.platformMessageId.length > 32 ||
        [part.acceptance.accountId, part.acceptance.to, part.acceptance.threadId ?? ""].some(
          (value) => typeof value !== "string" || value.length > 256,
        ),
    )
  ) {
    return {
      ...base,
      status: "UNKNOWN",
      reportDelivery: "UNKNOWN",
      reason: "receipt_export_limit",
    };
  }
  const first = evidence.acceptedParts[0].acceptance;
  const complete = evidence.completeReceipt;
  return {
    ...base,
    source: "committed-subagent-delivery",
    status: complete ? "complete" : "partial",
    reportDelivery: complete ? "ACCEPTED_COMPLETE" : "UNKNOWN",
    channel: first.channel,
    accountId: first.accountId,
    to: first.to,
    threadId: first.threadId,
    platformMessageIds: evidence.acceptedParts.map((part) => part.platformMessageId),
    acceptedAtMs: Math.max(...evidence.acceptedParts.map((part) => part.acceptance.acceptedAtMs)),
    parts: evidence.acceptedParts.map((part) => ({
      platformMessageId: part.platformMessageId,
      acceptedAtMs: part.acceptance.acceptedAtMs,
    })),
    ...(complete
      ? { resultHash: complete.result.resultHash, utf8ByteLength: complete.result.utf8ByteLength }
      : {}),
  };
}
