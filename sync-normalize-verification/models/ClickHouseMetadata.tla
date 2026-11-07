----------------------------- MODULE ClickHouseMetadata -----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Batches, Tables, GroupBatches, CopyBatches, MaxFailures
VARIABLE st
vars == <<st>>
Max(a,b) == IF a>b THEN a ELSE b
Min(a,b) == IF a<b THEN a ELSE b
\* Partial checkpoints are reached through actual modeled calls, not arbitrary initialization.
Init == \E d \in {0}: \E n \in 0..d: \E raw \in n..d:
 \E saved \in [Tables -> n..raw]:
 \E contents \in [1..Batches -> SUBSET Tables]:
 st=[batchTables |-> contents, ds |-> d, dn |-> n, rawCopied |-> raw, tableSaved |-> saved,
 phase |-> "idle", target |-> 0, lastNorm |-> 0, end |-> 0,
 cursor |-> 0, copyEnd |-> 0, jobs |-> {}, failures |-> 0]
\* FinishBatch advances sync; calls do not overlap for normalization.
\* Sync abstracts durable completion plus request publication. Calls capture its latest value.
Sync == /\ st.ds<Batches /\ st'=[st EXCEPT !.ds=@+1]
BeginCall == /\ st.phase="idle" /\ st.dn<st.ds
 /\ \E requested \in {st.ds}:
 st'=[st EXCEPT !.target=requested, !.lastNorm=st.dn,
 !.end=Min(requested,st.dn+GroupBatches), !.phase="readRaw", !.jobs={}]
ReadRaw == /\ st.phase="readRaw"
 /\ st'=[st EXCEPT !.cursor=Max(st.rawCopied,st.lastNorm), !.phase="copy"]
CopyRange == /\ st.phase="copy" /\ st.cursor<st.end
 /\ st'=[st EXCEPT !.copyEnd=Min(st.cursor+CopyBatches,st.end), !.phase="saveRaw"]
SaveRaw == /\ st.phase="saveRaw"
 /\ st'=[st EXCEPT !.rawCopied=st.copyEnd, !.cursor=st.copyEnd, !.phase="copy"]
\* Fixed batch contents preserve affected-table identity across failed calls and retries.
AffectedTables == UNION {st.batchTables[b]: b \in (st.lastNorm+1)..st.end}
DispatchTables == /\ st.phase="copy" /\ st.cursor>=st.end
 /\ st'=[st EXCEPT !.jobs={t \in AffectedTables:Max(st.lastNorm,st.tableSaved[t])<st.end}, !.phase="tables"]
\* Each distinct table is dispatched once; its INSERT precedes its checkpoint.
SaveTable(t) == /\ st.phase="tables" /\ t \in st.jobs
 /\ st'=[st EXCEPT !.tableSaved[t]=st.end, !.jobs=@ \ {t}]
Join == /\ st.phase="tables" /\ st.jobs={}
 /\ st'=[st EXCEPT !.phase="global"]
SaveGlobal == /\ st.phase="global"
 /\ st'=[st EXCEPT !.dn=st.end, !.phase="returned"]
Return == /\ st.phase="returned" /\ st'=[st EXCEPT !.phase="idle"]
\* Failure can follow any committed prefix, including the global write.
Fail == /\ st.phase \notin {"idle","failed","retry"} /\ st.failures<MaxFailures
 /\ st'=[st EXCEPT !.phase="failed", !.failures=@+1]
\* Workers already executing may still save their checkpoint during cancellation.
LateSaveTable(t) == /\ st.phase="failed" /\ t \in st.jobs
 /\ st'=[st EXCEPT !.tableSaved[t]=st.end, !.jobs=@ \ {t}]
JoinFailed == /\ st.phase="failed"
 /\ st'=[st EXCEPT !.jobs={}, !.phase="retry"]
RetryOrRestart == /\ st.phase="retry"
 /\ st'=[st EXCEPT !.phase="idle", !.target=0, !.lastNorm=0, !.end=0, !.cursor=0, !.copyEnd=0]
Done == /\ st.phase="idle" /\ st.dn=st.ds /\ UNCHANGED st
Next == Sync \/ BeginCall \/ ReadRaw \/ CopyRange \/ SaveRaw \/ DispatchTables
 \/ (\E t \in Tables:SaveTable(t) \/ LateSaveTable(t)) \/ Join \/ SaveGlobal \/ Return \/ Fail \/ JoinFailed \/ RetryOrRestart \/ Done
Spec == Init /\ [][Next]_vars
DurableProgressMonotone == [][st'.ds>=st.ds /\ st'.dn>=st.dn /\ st'.rawCopied>=st.rawCopied
 /\ (\A t \in Tables:st'.tableSaved[t]>=st.tableSaved[t])]_vars
DurableWriteArgumentsOrdered == [][(SaveRaw => st.copyEnd>=st.rawCopied)
 /\ (SaveGlobal => st.end>=st.dn)
 /\ (\A t \in Tables:(SaveTable(t) \/ LateSaveTable(t)) => st.end>=st.tableSaved[t])]_vars
\* Unaffected tables may lag global completion; only upper bounds hold for saved counters.
CheckpointsOrdered == st.dn<=st.rawCopied /\ st.rawCopied<=st.ds
 /\ (\A t \in Tables:st.tableSaved[t]<=st.rawCopied)
=============================================================================
