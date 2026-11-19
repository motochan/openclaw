import { describe, expect, it } from "vitest";
import { collectCompletionTransportEvidence } from "./subagent-completion-transport-evidence.js";
const base = {childRunId:"run-exact", childSessionKey:"child", requesterSessionKey:"parent",
 target:{channel:"telegram",to:"test-dm"},results:[{channel:"telegram",messageId:"platform-1"}],complete:true} as const;
describe("completion transport custody", () => {
 it("retains exact selected run and identified platform receipt", () => {
  expect(collectCompletionTransportEvidence(base)).toMatchObject({childRunId:"run-exact",platformMessageIds:["platform-1"],scope:"complete"});
 });
 it("keeps first-part success partial", () => expect(collectCompletionTransportEvidence({...base,complete:false})?.scope).toBe("partial"));
 it("does not infer a run from child session or label", () => expect(collectCompletionTransportEvidence({...base,childRunId:undefined})).toBeUndefined());
 it.each(["", "unknown", "suppressed"])("rejects missing/sentinel platform identity %s", messageId => expect(collectCompletionTransportEvidence({...base,results:[{channel:"telegram",messageId}]})).toBeUndefined());
 it("rejects wrong-target and not-sent results", () => {
  expect(collectCompletionTransportEvidence({...base,results:[{...base.results[0],target:{kind:"chat",id:"other"}}]})).toBeUndefined();
  expect(collectCompletionTransportEvidence({...base,results:[{...base.results[0],outcome:"not_sent"}]})).toBeUndefined();
 });
});
