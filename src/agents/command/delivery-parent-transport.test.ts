// Native owner attribution only; no Gateway boots/sends. Cost unmeasured: not run this slice.
import { describe, expect, it } from "vitest";
import {
  readCompletionBatchMessageTransport,
  withCompletionBatchDeliveryContext,
  type AgentCommandDeliveryStatus,
} from "./delivery-result.js";

const context = {
  parentRunId: "parent-run",
  requesterSessionKey: "agent:main:main",
  requesterSessionId: "session",
  admittedRunIds: ["included", "omitted", "settled-but-not-selected"],
  representedRunIds: ["included"],
  omittedRunIds: ["omitted"],
  isCurrent: () => true,
};
const status: AgentCommandDeliveryStatus = {
  requested: true,
  attempted: true,
  status: "sent",
  succeeded: true,
  resultCount: 1,
  platformMessageIds: ["actual-platform-id"],
};
const read = (parentRunId = context.parentRunId) =>
  readCompletionBatchMessageTransport({
    parentRunId,
    requesterSessionKey: context.requesterSessionKey,
    requesterSessionId: context.requesterSessionId,
    status,
  });

describe("completion batch parent transport", () => {
  it("keeps represented distinct from settled and generic acknowledgement report delivery UNKNOWN", async () => {
    expect(read()).toBeUndefined();
    await withCompletionBatchDeliveryContext(context, async () => {
      // The final could be only 'acknowledged'; no body inference grants report delivery.
      const transport = read();
      expect(transport?.kind).toBe("completion-batch-message");
      expect(transport?.admittedRunIds).toEqual(context.admittedRunIds);
      expect(transport?.representedRunIds).toEqual(["included"]);
      expect(transport?.omittedRunIds).toEqual(["omitted"]);
      expect(transport?.reportDelivery).toBe("UNKNOWN");
      expect(transport).not.toHaveProperty("scope");
      expect(transport).not.toHaveProperty("childRunId");
    });
  });

  it("rejects wrong/stale/closed parent owners without diagnostic exceptions", async () => {
    let current = true;
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let retained: Promise<ReturnType<typeof read>> | undefined;
    await withCompletionBatchDeliveryContext({ ...context, isCurrent: () => current }, async () => {
      expect(read("other-parent")).toBeUndefined();
      retained = (async () => {
        await gate;
        return read();
      })();
      current = false;
      expect(read()).toBeUndefined();
    });
    current = true;
    release?.();
    expect(await retained).toBeUndefined();
    await withCompletionBatchDeliveryContext(
      {
        ...context,
        isCurrent: () => {
          throw new Error("revoked");
        },
      },
      async () => {
        expect(read()).toBeUndefined();
      },
    );
  });
});
