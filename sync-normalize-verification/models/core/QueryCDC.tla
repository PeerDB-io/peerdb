----------------------------- MODULE QueryCDC -----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Batches, Limit, MaxFailures, MaxRestarts, Files
VARIABLE st
vars == <<st>>
Init == \E synced \in 0..Batches: \E normalized \in 0..synced:
 st = [ds |-> synced, dn |-> normalized, req |-> 0, resp |-> 0, highest |-> 0,
       applied |-> (1..normalized) \X (1..Files), staged |-> ((normalized+1)..synced) \X (1..Files), skipped |-> {}, file |-> 1,
       sp |-> "subscribe", np |-> "initRead", ss |-> 0, sn |-> 0,
       batch |-> 0, target |-> 0, current |-> 0,
       rg |-> 0, ng |-> 0, rw |-> 0, nw |-> 0, rc |-> {}, nc |-> {}, sold |-> 0, nold |-> 0, nreturn |-> "check",
       failures |-> 0, restarts |-> 0, emptyPoll |-> FALSE, canceled |-> FALSE]
ReadStartup == /\ ~st.canceled /\ st.np = "initRead"
               /\ st' = [st EXCEPT !.ss = st.ds, !.sn = st.dn, !.np = "initResponse"]
SeedResponse == /\ ~st.canceled /\ st.np = "initResponse"
                /\ st' = [st EXCEPT !.resp = st.sn, !.np = "responseSwap", !.nreturn = "initRequest"]
SeedRequest == /\ ~st.canceled /\ st.np = "initRequest"
               /\ st' = IF st.ss > st.sn
                  THEN [st EXCEPT !.req = st.ss,
                        !.highest = IF st.ss > st.highest THEN st.ss ELSE st.highest, !.np = "requestSwap"]
                  ELSE [st EXCEPT !.np = "check"]
SubscribeSync == /\ ~st.canceled /\ st.sp = "subscribe"
                 /\ st' = [st EXCEPT !.nw = st.ng, !.sp = "check"]
CheckPressure == /\ ~st.canceled /\ st.sp = "check"
                 /\ st' = [st EXCEPT !.sp = IF st.ds-st.dn >= Limit THEN "sleep" ELSE "pull",
                           !.batch = st.ds+1]
WakeSync == /\ ~st.canceled /\ st.sp = "sleep" /\ st.nw \in st.nc
            /\ st' = [st EXCEPT !.sp = "subscribe"]
\* RecordQueryCDCSync(0): zero is a no-advancement sentinel; SQL uses GREATEST.
EmptyPollArgument == 0
SyncCheckpointAfter(value) == IF value>st.ds THEN value ELSE st.ds
EmptyPoll == /\ ~st.canceled /\ st.sp = "pull" /\ ~st.emptyPoll
             /\ st' = [st EXCEPT !.emptyPoll = TRUE, !.ds = SyncCheckpointAfter(EmptyPollArgument), !.sp = "subscribe"]
Stage == /\ ~st.canceled /\ st.sp = "pull" /\ st.batch <= Batches
         /\ st' = [st EXCEPT !.staged = @ \cup ({st.batch} \X (1..Files)), !.sp = "finishPull"]
FinishPull == /\ ~st.canceled /\ st.sp = "finishPull"
              /\ st' = [st EXCEPT !.sp = "commit"]
CommitSync == /\ ~st.canceled /\ st.sp = "commit"
              /\ st' = [st EXCEPT !.ds = st.batch, !.sp = "publish"]
PublishSync == /\ ~st.canceled /\ st.sp = "publish"
               /\ st' = [st EXCEPT !.req = st.batch,
                         !.highest = IF st.batch > st.highest THEN st.batch ELSE st.highest,
                         !.sp = "publishSwap"]
FailPull == /\ ~st.canceled /\ st.sp = "finishPull" /\ st.failures < MaxFailures
            /\ st' = [st EXCEPT !.failures = @+1, !.sp = "subscribe"]
CheckWork == /\ ~st.canceled /\ st.np = "check"
             /\ st' = IF st.req <= st.resp THEN [st EXCEPT !.np = "subscribe"]
                      ELSE [st EXCEPT !.target = st.req, !.current = st.resp+1, !.np = "process"]
SubscribeNorm == /\ ~st.canceled /\ st.np = "subscribe"
                 /\ st' = [st EXCEPT !.rw = st.rg, !.np = "sleep"]
WakeNorm == /\ ~st.canceled /\ st.np = "sleep" /\ st.rw \in st.rc
            /\ st' = [st EXCEPT !.np = "check"]
SkipMissing == /\ ~st.canceled /\ st.np = "process" /\ st.current <= st.target
               /\ ~(\E f \in 1..Files: <<st.current,f>> \in st.staged)
               /\ st' = [st EXCEPT !.skipped = @ \cup {st.current}, !.current = @+1]
Apply(f) == /\ ~st.canceled /\ st.np = "process" /\ st.current <= st.target
         /\ <<st.current,f>> \in st.staged /\ (\A earlier \in 1..(f-1): <<st.current,earlier>> \notin st.staged)
         /\ st' = [st EXCEPT !.applied = @ \cup {<<st.current,f>>}, !.file = f, !.np = "delete"]
DeleteStage == /\ ~st.canceled /\ st.np = "delete"
               /\ st' = [st EXCEPT !.staged = @ \ {<<st.current,st.file>>}, !.current = IF st.file = Files THEN @+1 ELSE @, !.np = "process"]
FinishRange == /\ ~st.canceled /\ st.np = "process" /\ st.current > st.target
               /\ st' = [st EXCEPT !.np = "commit"]
CommitNorm == /\ ~st.canceled /\ st.np = "commit"
              /\ st' = [st EXCEPT !.dn = IF st.target > st.dn THEN st.target ELSE st.dn, !.np = "publish"]
ReportNorm == /\ ~st.canceled /\ st.np = "publish"
              /\ st' = [st EXCEPT !.resp = st.target, !.np = "responseSwap", !.nreturn = "check"]
FailNorm == /\ ~st.canceled /\ st.np \in {"process", "delete"}
            /\ st.failures < MaxFailures
            /\ st' = [st EXCEPT !.failures = @+1, !.np = "check"]
\* Catalog-write failure exits the activity; a subsequent attempt is represented by Restart, not FailNorm.
Restart == /\ ~st.canceled /\ st.restarts < MaxRestarts
           /\ st' = [st EXCEPT !.req = 0, !.resp = 0, !.highest = 0,
                     !.sp = "subscribe", !.np = "initRead", !.ss = 0, !.sn = 0,
                     !.batch = 0, !.target = 0, !.current = 0,
                     !.rg = 0, !.ng = 0, !.rw = 0, !.nw = 0, !.rc = {}, !.nc = {}, !.sold = 0, !.nold = 0, !.nreturn = "check", !.restarts = @+1]
Cancel == /\ ~st.canceled /\ st' = [st EXCEPT !.canceled = TRUE]
StopSync == /\ st.canceled /\ st.sp # "stopped" /\ st' = [st EXCEPT !.sp = "stopped"]
StopNorm == /\ st.canceled /\ st.np # "stopped" /\ st' = [st EXCEPT !.np = "stopped"]
Idle == /\ ((st.ds = Batches /\ st.dn = Batches /\ st.sp = "pull")
             \/ (st.sp = "stopped" /\ st.np = "stopped")) /\ UNCHANGED st
SyncSwap == /\ ~st.canceled /\ st.sp = "publishSwap"
 /\ st' = [st EXCEPT !.sold = st.rg, !.rg = @+1, !.sp = "publishClose"]
SyncClose == /\ ~st.canceled /\ st.sp = "publishClose"
 /\ st' = [st EXCEPT !.rc = @ \cup {st.sold}, !.sp = "subscribe"]
ResponseSwap == /\ ~st.canceled /\ st.np = "responseSwap"
 /\ st' = [st EXCEPT !.nold = st.ng, !.ng = @+1, !.np = "responseClose"]
ResponseClose == /\ ~st.canceled /\ st.np = "responseClose"
 /\ st' = [st EXCEPT !.nc = @ \cup {st.nold}, !.np = st.nreturn]
RequestSwap == /\ ~st.canceled /\ st.np = "requestSwap"
 /\ st' = [st EXCEPT !.nold = st.rg, !.rg = @+1, !.np = "requestClose"]
RequestClose == /\ ~st.canceled /\ st.np = "requestClose"
 /\ st' = [st EXCEPT !.rc = @ \cup {st.nold}, !.np = "check"]
Next == SyncSwap \/ SyncClose \/ ResponseSwap \/ ResponseClose \/ RequestSwap \/ RequestClose \/  ReadStartup \/ SeedResponse \/ SeedRequest \/ SubscribeSync \/ CheckPressure \/ WakeSync
        \/ EmptyPoll \/ Stage \/ FinishPull \/ CommitSync \/ PublishSync \/ FailPull
        \/ CheckWork \/ SubscribeNorm \/ WakeNorm \/ SkipMissing \/ (\E f \in 1..Files: Apply(f)) \/ DeleteStage
        \/ FinishRange \/ CommitNorm \/ ReportNorm \/ FailNorm \/ Restart
        \/ Cancel \/ StopSync \/ StopNorm \/ Idle
Spec == Init /\ [][Next]_vars
 /\ WF_vars(SyncSwap) /\ WF_vars(SyncClose) /\ WF_vars(ResponseSwap) /\ WF_vars(ResponseClose) /\ WF_vars(RequestSwap) /\ WF_vars(RequestClose)
 /\ WF_vars(ReadStartup) /\ WF_vars(SeedResponse) /\ WF_vars(SeedRequest)
 /\ WF_vars(SubscribeSync) /\ WF_vars(CheckPressure) /\ WF_vars(WakeSync)
 /\ WF_vars(Stage) /\ WF_vars(FinishPull) /\ WF_vars(CommitSync) /\ WF_vars(PublishSync)
 /\ WF_vars(CheckWork) /\ WF_vars(SubscribeNorm) /\ WF_vars(WakeNorm)
 /\ WF_vars(SkipMissing) /\ (\A f \in 1..Files: WF_vars(Apply(f))) /\ WF_vars(DeleteStage)
 /\ WF_vars(FinishRange) /\ WF_vars(CommitNorm) /\ WF_vars(ReportNorm)
 /\ WF_vars(StopSync) /\ WF_vars(StopNorm)
NoSkippedAcknowledgment == ((1..st.dn) \X (1..Files) \subseteq st.applied) /\ ((1..st.resp) \X (1..Files) \subseteq st.applied)
MissingMeansApplied == st.skipped \X (1..Files) \subseteq st.applied
PublishedWorkRecoverable == (1..st.ds) \X (1..Files) \subseteq (st.staged \cup st.applied)
ProgressOrdered == st.resp <= st.dn /\ st.dn <= st.ds /\ st.req <= st.ds
NoRequestRegression == st.req = st.highest
OnlyCommittedWorkApplied == st.applied \subseteq (1..st.ds) \X (1..Files)
NoStrandedNormalize == ~(~st.canceled /\ st.sp = "pull" /\ st.ds = Batches
                         /\ st.np = "sleep" /\ st.rw \notin st.rc /\ st.ds > st.dn)
NoStrandedSync == ~(~st.canceled /\ st.sp="sleep" /\ st.ds-st.dn<Limit
 /\ st.nw \notin st.nc /\ st.np="sleep")
CompletionReleasesSync == (~st.canceled /\ st.sp="sleep" /\ st.ds-st.dn<Limit)
 ~> (st.sp # "sleep" \/ st.canceled)
AllCommittedComplete == \A b \in 1..Batches: (st.ds >= b) ~> (st.dn >= b \/ st.canceled)
ShutdownCompletes == st.canceled ~> (st.sp = "stopped" /\ st.np = "stopped")

BoundaryExact == [][CheckPressure =>
 (st'.sp = "sleep" <=> st.ds-st.dn >= Limit)]_vars
EmptyPollDoesNotPublish == [][EmptyPoll => UNCHANGED <<st.ds,st.req,st.staged>>]_vars
FailedPullDoesNotPublish == [][FailPull => UNCHANGED <<st.ds,st.req>>]_vars
FailureDoesNotAcknowledge == [][FailNorm => UNCHANGED <<st.dn,st.resp>>]_vars
PublishAfterCommit == [][PublishSync => st.ds >= st.batch]_vars
CompletionBeforeNotification == st.resp <= st.dn

\* Separate per-table configuration: one shared permit, disjoint progress.
TableInit == st=[completed |-> [t \in {"a","b","c"} |-> 0], applied |-> [t \in {"a","b","c"} |-> 0],
 holder |-> "none", phase |-> [t \in {"a","b","c"} |-> "ready"], canceled |-> FALSE, fatal |-> FALSE]
AcquireTable(t) == /\ ~st.canceled /\ st.holder="none" /\ st.phase[t]="ready" /\ st.completed[t]<Batches
 /\ st'=[st EXCEPT !.holder=t, !.phase[t]="io"]
ApplyTable(t) == /\ st.holder=t /\ st.phase[t]="io"
 /\ st'=[st EXCEPT !.applied[t]=@+1, !.phase[t]="release"]
ReleaseTable(t) == /\ st.holder=t /\ st.phase[t]="release"
 /\ st'=[st EXCEPT !.holder="none", !.phase[t]="commit"]
CommitTable(t) == /\ st.phase[t]="commit"
 /\ st'=[st EXCEPT !.completed[t]=st.applied[t], !.phase[t]=IF st.canceled THEN "stopped" ELSE "ready"]
FatalTableError(t) == /\ ~st.canceled /\ MaxFailures>0 /\ st.phase[t]="commit"
 /\ st'=[st EXCEPT !.phase[t]="stopped", !.canceled=TRUE, !.fatal=TRUE]
StopTable(t) == /\ st.canceled /\ st.phase[t] \in {"ready","commit"}
 /\ st'=[st EXCEPT !.phase[t]="stopped"]
TablesStopped == \A t \in {"a","b","c"}:st.phase[t]="stopped"
TableDone == /\ ((\A t \in {"a","b","c"}:st.completed[t]=Batches) \/ TablesStopped) /\ UNCHANGED st
TableNext == (\E t \in {"a","b","c"}:AcquireTable(t) \/ ApplyTable(t) \/ ReleaseTable(t) \/ CommitTable(t) \/ FatalTableError(t) \/ StopTable(t)) \/ TableDone
TableSpec == TableInit /\ [][TableNext]_vars
 /\ (\A t \in {"a","b","c"}:SF_vars(AcquireTable(t)) /\ WF_vars(ApplyTable(t)) /\ WF_vars(ReleaseTable(t)) /\ WF_vars(CommitTable(t)) /\ WF_vars(StopTable(t)))
TableAckMatchesOwnWork == \A t \in {"a","b","c"}:st.completed[t]<=st.applied[t]
TableCommitIsIsolated == [][\A t \in {"a","b","c"}:CommitTable(t) => (\A other \in {"a","b","c"}\{t}: st'.completed[other]=st.completed[other])]_vars
EveryTableCompletesWithFairAdmission == <>(st.canceled \/ (\A t \in {"a","b","c"}:st.completed[t]=Batches))
TableCommitHoldsNoSlot == \A t \in {"a","b","c"}:st.phase[t]="commit" => st.holder # t
FatalTableErrorStopsAllTables == st.fatal ~> TablesStopped
TableShutdownReleasesSlot == TablesStopped => st.holder="none"

CommittedProgressEventuallyVisible == \A b \in 1..Batches:(st.ds >= b /\ st.dn < b) ~> (st.req >= b \/ st.dn >= b \/ st.canceled)

\* Startup may seed completion before requests: readiness is durable, not req>=resp.
AcknowledgmentRequiresReadySync == st.resp<=st.ds /\ (1..st.resp) \X (1..Files) \subseteq st.applied
AcknowledgmentWithinTarget == [][ReportNorm => st'.resp<=st.target]_vars
\* The baseline checks the durable catalog pair, not LastChan values.
BackpressureStopsFurtherPulls == [][Stage => st.ds-st.dn<Limit]_vars
DurableProgressMonotone == [][st'.ds>=st.ds /\ st'.dn>=st.dn]_vars
DurableWriteArgumentsOrdered == [][(CommitSync => st.batch>=st.ds) /\ (CommitNorm => st.target>=st.dn) /\ (EmptyPoll => (EmptyPollArgument=0 /\ st'.ds=st.ds))]_vars
=============================================================================
