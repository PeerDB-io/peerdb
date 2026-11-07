----------------------------- MODULE ClickHouseMetadata -----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Batches, Tables, GroupBatches, CopyBatches, MaxCalls, MaxFailures
VARIABLE st
vars == <<st>>
Max(a,b) == IF a>b THEN a ELSE b
Min(a,b) == IF a<b THEN a ELSE b
Init == \E d \in {0}: \E n \in 0..d: \E raw \in n..d:
 \E saved \in [Tables -> n..raw]:
 st=[ds |-> d, dn |-> n, rawCopied |-> raw, tableSaved |-> saved,
 phase |-> "idle", target |-> 0, lastNorm |-> 0, end |-> 0,
 cursor |-> 0, copyEnd |-> 0, sent |-> {}, pending |-> {}, calls |-> 0, failures |-> 0]
\* One owner calls NormalizeRecords serially; child tasks may outlive its return.
\* Sync abstracts durable completion plus request publication. Calls capture its latest value.
Sync == /\ st.ds<Batches /\ st'=[st EXCEPT !.ds=@+1]
BeginCall == /\ st.phase="idle" /\ st.dn<st.ds /\ st.calls<MaxCalls
 /\ \E requested \in {st.ds}:
 st'=[st EXCEPT !.target=requested, !.lastNorm=st.dn,
 !.end=Min(requested,st.dn+GroupBatches), !.phase="readRaw", !.sent={}, !.calls=@+1]
ReadRaw == /\ st.phase="readRaw"
 /\ st'=[st EXCEPT !.cursor=Max(st.rawCopied,st.lastNorm), !.phase="copy"]
CopyRange == /\ st.phase="copy" /\ st.cursor<st.end
 /\ st'=[st EXCEPT !.copyEnd=Min(st.cursor+CopyBatches,st.end), !.phase="saveRaw"]
SaveRaw == /\ st.phase="saveRaw"
 /\ st'=[st EXCEPT !.rawCopied=st.copyEnd, !.cursor=st.copyEnd, !.phase="copy"]
StartTables == /\ st.phase="copy" /\ st.cursor>=st.end
 /\ st'=[st EXCEPT !.phase="tables"]
\* Track two affected tables. A later third table can fail producer metadata/query generation.
Dispatch(t) == /\ st.phase="tables" /\ t \notin st.sent
 /\ st'=[st EXCEPT !.sent=@ \cup {t},
 !.pending=IF Max(st.lastNorm,st.tableSaved[t])<st.end THEN @ \cup {<<st.calls,t,st.end>>} ELSE @]
\* INSERT completes before this SQL assignment. Neither early return nor primary Close joins a separate connection.
SaveTable(job) == /\ job \in st.pending
 /\ st'=[st EXCEPT !.tableSaved[job[2]]=job[3], !.pending=@ \ {job}]
CurrentJobs == {j \in st.pending:j[1]=st.calls}
Join == /\ st.phase="tables" /\ st.sent=Tables /\ CurrentJobs={}
 /\ st'=[st EXCEPT !.phase="global"]
SaveGlobal == /\ st.phase="global"
 /\ st'=[st EXCEPT !.dn=st.end, !.phase="returned"]
Return == /\ st.phase="returned" /\ st'=[st EXCEPT !.phase="idle"]
ProducerError == /\ st.phase="tables" /\ st.sent=Tables /\ st.failures<MaxFailures
 /\ st'=[st EXCEPT !.phase="retry", !.failures=@+1]
FailOutsideWorkers == /\ st.phase \in {"readRaw","copy","saveRaw","global","returned"} /\ st.failures<MaxFailures
 /\ st'=[st EXCEPT !.phase="retry", !.failures=@+1]
Retry == /\ st.phase="retry"
 /\ st'=[st EXCEPT !.phase="idle", !.target=0, !.lastNorm=0, !.end=0, !.cursor=0, !.copyEnd=0]
\* Process loss kills child tasks, unlike an ordinary early function return.
CrashRestart == /\ st.phase # "idle" /\ st.failures<MaxFailures
 /\ st'=[st EXCEPT !.phase="idle", !.pending={}, !.failures=@+1,
 !.target=0, !.lastNorm=0, !.end=0, !.cursor=0, !.copyEnd=0]
Done == /\ st.phase="idle" /\ (st.dn=st.ds \/ st.calls=MaxCalls) /\ st.pending={} /\ UNCHANGED st
Next == Sync \/ BeginCall \/ ReadRaw \/ CopyRange \/ SaveRaw \/ StartTables
 \/ (\E t \in Tables:Dispatch(t)) \/ (\E j \in st.pending:SaveTable(j))
 \/ Join \/ SaveGlobal \/ Return \/ ProducerError \/ FailOutsideWorkers \/ Retry \/ CrashRestart \/ Done
Spec == Init /\ [][Next]_vars
DurableProgressMonotone == [][st'.ds>=st.ds /\ st'.dn>=st.dn /\ st'.rawCopied>=st.rawCopied
 /\ (\A t \in Tables:st'.tableSaved[t]>=st.tableSaved[t])]_vars
DurableWriteArgumentsOrdered == [][(SaveRaw => st.copyEnd>=st.rawCopied)
 /\ (SaveGlobal => st.end>=st.dn)
 /\ (\A j \in st.pending:SaveTable(j) => j[3]>=st.tableSaved[j[2]])]_vars
SerialCheckpointsMonotone == [][st'.ds>=st.ds /\ st'.dn>=st.dn /\ st'.rawCopied>=st.rawCopied]_vars
=============================================================================
