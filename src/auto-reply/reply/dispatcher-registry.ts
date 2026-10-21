/**
 * Global registry for tracking active reply dispatchers.
 * Used to ensure gateway restart waits for all replies to complete.
 */
import { resolveGlobalSet } from "../../shared/global-singleton.js";

type TrackedDispatcher = {
  readonly pending: () => number;
};

const activeDispatchers = resolveGlobalSet<TrackedDispatcher>(
  Symbol.for("openclaw.activeReplyDispatchers"),
  "close-only",
);

/**
 * Returns an unregister function to call when the dispatcher is no longer needed.
 */
export function registerDispatcher(pending: () => number): () => void {
  // Separate registrations must remain distinct even when they share a callback.
  const tracked: TrackedDispatcher = { pending };
  activeDispatchers.add(tracked);

  return () => {
    activeDispatchers.delete(tracked);
  };
}

export function getTotalPendingReplies(options: { strict?: boolean } = {}): number {
  let total = 0;
  for (const dispatcher of activeDispatchers) {
    const pending = dispatcher.pending();
    if (options.strict && (!Number.isSafeInteger(pending) || pending < 0)) {
      return Number.NaN;
    }
    total += pending;
  }
  return total;
}
