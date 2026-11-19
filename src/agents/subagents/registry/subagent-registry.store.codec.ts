import { safeParseJson } from "@openclaw/normalization-core";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import type { Selectable } from "kysely";
import type { DB as OpenClawStateKyselyDatabase } from "../../../state/openclaw-state-db.generated.js";
import { normalizeDeliveryContext } from "../../../utils/delivery-context.shared.js";
import type { SubagentAnnounceDeliveryResult } from "../announce/subagent-announce-dispatch.js";
import type { CompletionTransportEvidence } from "../announce/subagent-completion-transport-evidence.js";
import { normalizeSubagentRunState } from "./subagent-delivery-state.js";
import type { BoundSubagentRunRecord } from "./subagent-registry.store.kernel.js";
import type { SubagentRunRecord } from "./subagent-registry.types.js";

/** Validate exact owner identities and route, never labels, timestamps or transcript text. */
export function isBoundCompletionTransportEvidence(
  entry: SubagentRunRecord,
  value: unknown,
): value is CompletionTransportEvidence & { durable?: true } {
  if (!isRecord(value)) return false;
  const origin = entry.requesterOrigin;
  return (
    value.childRunId === entry.runId &&
    value.childSessionKey === entry.childSessionKey &&
    value.requesterSessionKey === entry.requesterSessionKey &&
    typeof value.channel === "string" &&
    !!value.channel &&
    typeof value.to === "string" &&
    !!value.to &&
    !!origin &&
    origin.channel === value.channel &&
    origin.to === value.to &&
    (origin.accountId ?? undefined) === (value.accountId ?? undefined) &&
    (origin.threadId == null ? undefined : String(origin.threadId)) ===
      (value.threadId ?? undefined) &&
    (value.scope === "partial" || value.scope === "complete") &&
    Array.isArray(value.platformMessageIds) &&
    value.platformMessageIds.length > 0 &&
    value.platformMessageIds.length <= 100 &&
    value.platformMessageIds.every(
      (id) =>
        typeof id === "string" &&
        !!id.trim() &&
        id.length <= 1024 &&
        id !== "unknown" &&
        id !== "suppressed",
    )
  );
}

/** Called only by existing completion owners after their authority checks. */
export function retainCompletionTransportEvidence(
  entry: SubagentRunRecord,
  outcome: SubagentAnnounceDeliveryResult,
): boolean {
  const evidence = outcome.completionTransportEvidence;
  if (
    !outcome.delivered ||
    outcome.path !== "direct" ||
    !isBoundCompletionTransportEvidence(entry, evidence)
  )
    return false;
  if (entry.delivery?.completionTransportEvidence?.scope === "complete") return false;
  entry.delivery ??= { status: "pending" };
  // A callback is tentative until the canonical owner commits and reads it back.
  entry.delivery.completionTransportEvidence = {
    childRunId: evidence.childRunId,
    childSessionKey: evidence.childSessionKey,
    requesterSessionKey: evidence.requesterSessionKey,
    channel: evidence.channel,
    to: evidence.to,
    ...(evidence.accountId ? { accountId: evidence.accountId } : {}),
    ...(evidence.threadId ? { threadId: evidence.threadId } : {}),
    platformMessageIds: [...evidence.platformMessageIds],
    scope: evidence.scope,
  };
  return true;
}

/** Invoked only by the maintained writer after its exact commit acknowledgement. */
export function publishCommittedCompletionTransportEvidence(entry: SubagentRunRecord): void {
  const evidence = entry.delivery?.completionTransportEvidence;
  if (evidence && isBoundCompletionTransportEvidence(entry, evidence)) {
    evidence.durable = true;
  }
}

/** Only a canonical store readback can publish durable=true. */
export function confirmCompletionTransportEvidenceReadback(
  entry: SubagentRunRecord,
  stored: SubagentRunRecord | null,
): void {
  const evidence = entry.delivery?.completionTransportEvidence;
  const receipt = stored?.delivery?.completionTransportEvidence;
  if (
    evidence &&
    receipt?.durable === true &&
    isBoundCompletionTransportEvidence(entry, receipt) &&
    JSON.stringify({ ...evidence, durable: undefined }) ===
      JSON.stringify({ ...receipt, durable: undefined })
  ) {
    evidence.durable = true;
  }
}

type SubagentRunsTable = OpenClawStateKyselyDatabase["subagent_runs"];
export type SubagentRunSqliteRow = Selectable<SubagentRunsTable>;
type CanonicalSubagentRunRecord = SubagentRunRecord &
  Required<Pick<SubagentRunRecord, "completion" | "delivery">>;
const EXECUTION_STATUSES = new Set("queued running interrupted terminal".split(" "));
export const DELIVERY_STATUSES = new Set(
  "not_required pending in_progress delivered failed suspended discarded".split(" "),
);

function hasStateStatus(
  value: unknown,
  statuses: ReadonlySet<string>,
): value is Record<string, unknown> {
  return isRecord(value) && typeof value.status === "string" && statuses.has(value.status);
}

function isCanonicalSubagentRunRecord(value: unknown): value is CanonicalSubagentRunRecord {
  return (
    isRecord(value) &&
    hasStateStatus(value.execution, EXECUTION_STATUSES) &&
    isRecord(value.completion) &&
    typeof value.completion.required === "boolean" &&
    hasStateStatus(value.delivery, DELIVERY_STATUSES) &&
    !(
      "handoffLeaseId" in value.delivery ||
      "handoffLeasedAt" in value.delivery ||
      "handoffInjectedAt" in value.delivery
    )
  );
}

function assertCanonicalSubagentRunRecord(
  entry: SubagentRunRecord,
): asserts entry is CanonicalSubagentRunRecord {
  if (!isCanonicalSubagentRunRecord(entry)) {
    throw new Error("subagent run is missing canonical nested state");
  }
}

function parseJson(raw: string | null): unknown {
  return raw ? safeParseJson(raw) : undefined;
}

/** Rehydrates one sqlite row into the normalized subagent run record shape. */
export function rowToSubagentRunRecord(row: SubagentRunSqliteRow): SubagentRunRecord | null {
  const stored = parseJson(row.payload_json);
  const payload =
    isRecord(stored) &&
    isRecord(stored.parentCompletion) &&
    stored.parentCompletion.completionTarget === "parent"
      ? stored.parentCompletion
      : stored;
  if (!isCanonicalSubagentRunRecord(payload)) {
    return null;
  }
  // Writers commit indexed columns with this complete payload atomically;
  // rehydrating both created competing state.
  payload.runId = row.run_id;
  payload.childSessionKey = row.child_session_key;
  payload.requesterSessionKey = row.requester_session_key;
  payload.requesterStorePath = row.requester_store_path ?? undefined;
  payload.controllerStorePath = row.controller_store_path ?? undefined;
  const controllerSessionKey = row.controller_session_key?.trim();
  if (controllerSessionKey) {
    payload.controllerSessionKey = controllerSessionKey;
  } else {
    delete payload.controllerSessionKey;
  }
  if (payload.requesterOrigin) {
    payload.requesterOrigin = normalizeDeliveryContext(payload.requesterOrigin);
  }
  if (payload.expectsCompletionMessage === false) {
    payload.delivery.status = "not_required";
  }
  // Indexed identity is authoritative, including when a damaged payload disagrees.
  if (
    payload.delivery.completionTransportEvidence &&
    !isBoundCompletionTransportEvidence(payload, payload.delivery.completionTransportEvidence)
  ) {
    delete payload.delivery.completionTransportEvidence;
  }
  const record = normalizeSubagentRunState(payload);
  if (record.delivery?.completionTransportEvidence) {
    const evidence = record.delivery.completionTransportEvidence;
    // Decode to the allowlist too: stored unknown keys must never leak into tool metadata.
    delete record.delivery.completionTransportEvidence;
    retainCompletionTransportEvidence(record, {
      delivered: true,
      path: "direct",
      completionTransportEvidence: evidence,
    });
    record.delivery.completionTransportEvidence!.durable = true;
  }
  return record.runId && record.childSessionKey && record.requesterSessionKey ? record : null;
}

/** Canonically serializes a run before an outer transaction acquires the write lock. */
export function bindSubagentRunRecord(entry: SubagentRunRecord): BoundSubagentRunRecord {
  return bindMutableSubagentRunRecord(structuredClone(entry));
}

/** Binds an isolated registry capture without copying its complete payload again. */
export function bindCapturedSubagentRunRecord(entry: SubagentRunRecord): BoundSubagentRunRecord {
  assertCanonicalSubagentRunRecord(entry);
  const completion = entry.completion;
  const hadTerminalReply = Object.hasOwn(completion, "terminalReply");
  const terminalReply = completion.terminalReply;
  try {
    // Preserve aliases during the second normalization, which can change text again.
    return bindMutableSubagentRunRecord({ ...entry });
  } finally {
    // Root writes use the copy; restore the sole nested write before capture publication.
    if (hadTerminalReply) {
      completion.terminalReply = terminalReply;
    } else {
      delete completion.terminalReply;
    }
  }
}

function bindMutableSubagentRunRecord(entry: SubagentRunRecord): BoundSubagentRunRecord {
  const normalized = normalizeSubagentRunState(entry);
  assertCanonicalSubagentRunRecord(normalized);
  if (normalized.delivery.completionTransportEvidence) {
    normalized.delivery = {
      ...normalized.delivery,
      completionTransportEvidence: {
        ...normalized.delivery.completionTransportEvidence,
        durable: undefined,
      },
    };
  }
  return {
    run_id: normalized.runId,
    child_session_key: normalized.childSessionKey,
    controller_session_key: normalized.controllerSessionKey?.trim() || null,
    requester_session_key: normalized.requesterSessionKey,
    requester_store_path: normalized.requesterStorePath ?? null,
    controller_store_path: normalized.controllerStorePath ?? null,
    created_at: normalized.createdAt,
    // Released readers require root execution/completion/delivery state. Hiding
    // the whole private record also excludes it from legacy mixed/nested summaries.
    // Downgrades may discard these rows, but cannot reinterpret them as public.
    payload_json: JSON.stringify(
      normalized.completionTarget === "parent" ? { parentCompletion: normalized } : normalized,
    ),
  };
}
