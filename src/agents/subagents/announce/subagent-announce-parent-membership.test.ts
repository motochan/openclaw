// Authored source-only; runtime cost not measured (execution deferred to maintained owner).
import { describe, expect, it } from "vitest";
import { buildAgentRunTerminalReplySnapshot } from "../../agent-run-terminal-reply.js";
import {
  buildChildCompletionFindings,
  buildChildCompletionFindingsWithRuns,
  type ChildCompletionRow,
} from "./subagent-announce-result.js";

const row = (runId: string): ChildCompletionRow => ({
  runId,
  childSessionKey: "agent:main:subagent:" + runId,
  task: runId,
  createdAt: 1,
  execution: { status: "terminal", outcome: { status: "ok" } },
  announceResult: "report " + runId,
});

describe("parent prompt material membership", () => {
  it("does not credit the successful captured silent child omitted by the formatter", () => {
    const omitted = row("omitted");
    omitted.announceResult = undefined;
    omitted.completion = {
      terminalReply: buildAgentRunTerminalReplySnapshot({ visibleText: "NO_REPLY" }),
    };
    const findings = buildChildCompletionFindingsWithRuns([row("included"), omitted]);
    expect(findings.representedRunIds).toEqual(["included"]);
    expect(findings.omittedRunIds).toEqual(["omitted"]);
    expect(findings.text).not.toContain("report omitted");
    expect(findings.text).toBe(buildChildCompletionFindings([row("included")]));
  });

  it("preserves legacy text and never turns unidentifiable formatting rows into run membership", () => {
    const legacy = row("legacy");
    delete legacy.runId;
    const rows = [row("physical-run"), legacy];
    const findings = buildChildCompletionFindingsWithRuns(rows);
    expect(findings.text).toBe(buildChildCompletionFindings(rows));
    expect(findings.representedRunIds).toEqual(["physical-run"]);
    expect(findings.unidentifiedRowCount).toBe(1);
  });
});
