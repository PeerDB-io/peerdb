----------------------------- MODULE QueryCDC -----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Batches, Limit, MaxFailures, MaxRestarts, MaxFatalFailures, Files
VARIABLE st
vars == <<st>>
InitialState(synced,normalized) == [available |-> synced, ds |-> synced, dn |-> normalized, req |-> synced, resp |-> normalized, highest |-> synced,
       applied |-> (1..normalized) \X (1..Files), staged |-> ((normalized+1)..synced) \X (1..Files), skipped |-> {}, file |-> 1,
       sp |-> "check", np |-> "check", ss |-> 0, sn |-> 0,
       batch |-> 0, target |-> 0, current |-> 0,
       g |-> 0, rw |-> 0, nw |-> 0, rc |-> {}, nc |-> {}, sold |-> 0, nold |-> 0, nreturn |-> "check",
       phase |-> "running", groupCanceled |-> FALSE, fatalFailures |-> 0, failures |-> 0, restarts |-> 0, emptyPoll |-> FALSE, canceled |-> FALSE]
InitialStates == UNION {{InitialState(s,n):n \in 0..s}:s \in 0..Batches}
Init == st \in InitialStates



CancelRequested == st.canceled \/ st.groupCanceled
Arrival == /\ st.available<Batches /\ st'=[st EXCEPT !.available=@+1]
CheckPressure == /\ ~CancelRequested /\ st.sp = "check"
 /\ st' = [st EXCEPT !.sp = IF st.req-st.resp >= Limit THEN "sleep" ELSE "pull", !.nw=st.g, !.batch=st.ds+1]
WakeSync == /\ ~CancelRequested /\ st.sp = "sleep" /\ st.nw < st.g
            /\ st' = [st EXCEPT !.sp = "check"]
\* RecordQueryCDCSync(0): zero is a no-advancement sentinel; SQL uses GREATEST.
EmptyPollArgument == 0
SyncCheckpointAfter(value) == IF value>st.ds THEN value ELSE st.ds
EmptyPoll == /\ ~CancelRequested /\ st.sp = "pull" /\ st.ds=st.available
             /\ st' = [st EXCEPT !.emptyPoll = TRUE, !.ds = SyncCheckpointAfter(EmptyPollArgument), !.sp = "check"]
BeginPull == /\ ~CancelRequested /\ st.sp="pull" /\ st.batch<=st.available
 /\ st'=[st EXCEPT !.sp="staging"]
Stage == /\ st.sp = "staging" /\ st.batch <= st.available
         /\ st' = [st EXCEPT !.staged = @ \cup ({st.batch} \X (1..Files)), !.sp = "finishPull"]
FinishPull == /\ st.sp = "finishPull"
              /\ st' = [st EXCEPT !.sp = "commit"]
CommitSync == /\ st.sp = "commit"
              /\ st' = [st EXCEPT !.ds = st.batch, !.sp = "publish"]
PublishSync == /\ st.sp = "publish"
 /\ st' = [st EXCEPT !.req = st.ds, !.highest=st.ds, !.g = IF st.ds > st.req THEN @+1 ELSE @, !.sp = "check"]
FailPull == /\ ~CancelRequested /\ st.sp = "finishPull" /\ st.failures < MaxFailures
            /\ st' = [st EXCEPT !.failures = @+1, !.sp = "check"]
CheckWork == /\ ~CancelRequested /\ st.np = "check"
 /\ st' = IF st.req <= st.resp THEN [st EXCEPT !.rw=st.g, !.np="sleep"]
 ELSE [st EXCEPT !.target=st.req, !.current=st.resp+1, !.np="acquire"]
WakeNorm == /\ ~CancelRequested /\ st.np = "sleep" /\ st.rw < st.g
            /\ st' = [st EXCEPT !.np = "check"]
AcquireNormConnector == /\ st.np="acquire" /\ st'=[st EXCEPT !.np="process"]
SkipMissing == /\ st.np = "process" /\ st.current <= st.target
               /\ ~(\E f \in 1..Files: <<st.current,f>> \in st.staged)
               /\ st' = [st EXCEPT !.skipped = @ \cup {st.current}, !.current = @+1]
Apply(f) == /\ st.np = "process" /\ st.current <= st.target
         /\ <<st.current,f>> \in st.staged /\ (\A earlier \in 1..(f-1): <<st.current,earlier>> \notin st.staged)
         /\ st' = [st EXCEPT !.applied = @ \cup {<<st.current,f>>}, !.file = f, !.np = "delete"]
DeleteStage == /\ st.np = "delete"
               /\ st' = [st EXCEPT !.staged = @ \ {<<st.current,st.file>>}, !.current = IF st.file = Files THEN @+1 ELSE @, !.np = "process"]
FinishRange == /\ st.np = "process" /\ st.current > st.target
               /\ st' = [st EXCEPT !.np = "commit"]
CommitNorm == /\ st.np = "commit"
              /\ st' = [st EXCEPT !.dn = IF st.target > st.dn THEN st.target ELSE st.dn, !.np = "publish"]
ReportNorm == /\ st.np = "publish"
 /\ st' = [st EXCEPT !.resp=st.target, !.g=IF st.target>st.resp THEN @+1 ELSE @, !.np="check"]
FailNorm == /\ ~CancelRequested /\ st.np \in {"process", "delete"}
            /\ st.failures < MaxFailures
            /\ st' = [st EXCEPT !.failures = @+1, !.np = "check"]
\* Fatal table-loop errors return the failed goroutine and cancel its siblings.
\* From publish, persistence committed but returned an error before monitor publication.
\* Successful persistence still permits publication despite sibling/parent cancellation.
FatalSyncError == /\ ~CancelRequested /\ st.sp \in {"pull","commit","publish"} /\ st.fatalFailures<MaxFatalFailures
 /\ st'=[st EXCEPT !.sp="stopped", !.groupCanceled=TRUE, !.phase="canceling", !.fatalFailures=@+1]
FatalNormError == /\ ~CancelRequested /\ st.np \in {"acquire","commit","publish"} /\ st.fatalFailures<MaxFatalFailures
 /\ st'=[st EXCEPT !.np="stopped", !.groupCanceled=TRUE, !.phase="canceling", !.fatalFailures=@+1]
CanceledSyncCommitError == /\ CancelRequested /\ st.sp="publish"
 /\ st'=[st EXCEPT !.sp="stopped"]
CanceledNormCommitError == /\ CancelRequested /\ st.np="publish"
 /\ st'=[st EXCEPT !.np="stopped"]
JoinGroup == /\ st.phase="canceling" /\ st.sp="stopped" /\ st.np="stopped"
 /\ st'=[st EXCEPT !.phase="returned"]
CrashActivity == /\ ~CancelRequested /\ st.phase="running"
 /\ st.restarts+(MaxFatalFailures-st.fatalFailures)<MaxRestarts
 /\ st'=[st EXCEPT !.sp="stopped", !.np="stopped", !.phase="returned", !.groupCanceled=TRUE]
Reconstructed == [st EXCEPT !.req=st.ds, !.resp=st.dn, !.highest=st.ds,
 !.sp="check", !.np="check", !.ss=0, !.sn=0, !.batch=0, !.target=0, !.current=0,
 !.g=0, !.rw=0, !.nw=0, !.rc={}, !.nc={}, !.sold=0, !.nold=0, !.nreturn="check",
 !.phase="running", !.groupCanceled=FALSE, !.restarts=@+1]
Restart == /\ ~st.canceled /\ st.phase="returned" /\ st.sp="stopped" /\ st.np="stopped"
 /\ st.restarts<MaxRestarts /\ st'=Reconstructed
Cancel == /\ ~st.canceled /\ st.phase # "returned"
 /\ st'=[st EXCEPT !.canceled=TRUE, !.phase="canceling"]
StopSync == /\ CancelRequested /\ st.sp \notin {"stopped","publish"} /\ st'=[st EXCEPT !.sp="stopped"]
StopNorm == /\ CancelRequested /\ st.np \notin {"stopped","publish"} /\ st'=[st EXCEPT !.np="stopped"]
Idle == /\ ((st.ds = st.available /\ st.dn = st.ds /\ st.sp = "pull")
             \/ (st.sp = "stopped" /\ st.np = "stopped")) /\ UNCHANGED st






Next == AcquireNormConnector \/ BeginPull \/ FatalSyncError \/ FatalNormError \/ JoinGroup \/ CrashActivity \/ Arrival \/ CheckPressure \/ WakeSync
        \/ EmptyPoll \/ Stage \/ FinishPull \/ CommitSync \/ PublishSync \/ FailPull
        \/ CheckWork \/ WakeNorm \/ SkipMissing \/ (\E f \in 1..Files: Apply(f)) \/ DeleteStage
        \/ FinishRange \/ CommitNorm \/ ReportNorm \/ FailNorm \/ Restart
        \/ Cancel \/ StopSync \/ StopNorm \/ CanceledSyncCommitError \/ CanceledNormCommitError \/ Idle
Spec == Init /\ [][Next]_vars


 /\ WF_vars(AcquireNormConnector) /\ WF_vars(BeginPull) /\ WF_vars(JoinGroup) /\ WF_vars(Restart)
 /\ WF_vars(CheckPressure) /\ WF_vars(WakeSync)
 /\ WF_vars(Stage) /\ WF_vars(FinishPull) /\ WF_vars(CommitSync) /\ WF_vars(PublishSync)
 /\ WF_vars(CheckWork) /\ WF_vars(WakeNorm)
 /\ WF_vars(SkipMissing) /\ (\A f \in 1..Files: WF_vars(Apply(f))) /\ WF_vars(DeleteStage)
 /\ WF_vars(FinishRange) /\ WF_vars(CommitNorm) /\ WF_vars(ReportNorm)
 /\ WF_vars(StopSync) /\ WF_vars(StopNorm)
NoSkippedAcknowledgment == ((1..st.dn) \X (1..Files) \subseteq st.applied) /\ ((1..st.resp) \X (1..Files) \subseteq st.applied)
MissingMeansApplied == st.skipped \X (1..Files) \subseteq st.applied
PublishedWorkRecoverable == (1..st.ds) \X (1..Files) \subseteq (st.staged \cup st.applied)
ProgressOrdered == st.resp <= st.dn /\ st.dn <= st.ds /\ st.req <= st.ds
NoRequestRegression == st.req = st.highest
OnlyCommittedWorkApplied == st.applied \subseteq (1..st.ds) \X (1..Files)
NoStrandedNormalize == ~(~CancelRequested /\ st.sp = "pull" /\ st.ds = st.available
                         /\ st.np = "sleep" /\ st.rw = st.g /\ st.ds > st.dn)
AllCommittedComplete == \A b \in 1..Batches: (st.ds >= b) ~> (st.dn >= b \/ st.canceled)
ShutdownCompletes == st.canceled ~> (st.phase="returned" /\ st.sp = "stopped" /\ st.np = "stopped")

BoundaryExact == [][CheckPressure =>
 (st'.sp = "sleep" <=> st.req-st.resp >= Limit)]_vars
EmptyPollDoesNotPublish == [][EmptyPoll => UNCHANGED <<st.ds,st.req,st.staged>>]_vars
FailedPullDoesNotPublish == [][FailPull => UNCHANGED <<st.ds,st.req>>]_vars
FailureDoesNotAcknowledge == [][FailNorm => UNCHANGED <<st.dn,st.resp>>]_vars
PublishAfterCommit == [][PublishSync => st.ds >= st.batch]_vars
ReadyCompletionCanPublish == st.np="publish" => ENABLED ReportNorm
ReadySyncCanPublish == st.sp="publish" => ENABLED PublishSync
ReturnedGroupHasNoLoops == st.phase="returned" => st.sp="stopped" /\ st.np="stopped"
FatalErrorsCancelGroup == [][(FatalSyncError \/ FatalNormError) => (st'.groupCanceled /\ st'.phase="canceling")]_vars
RestartRequiresGroupExit == [][Restart => (st.phase="returned" /\ st.sp="stopped" /\ st.np="stopped")]_vars
RestartSeedsDurableProgress == [][Restart => (st'.req=st.ds /\ st'.resp=st.dn /\ ~st'.groupCanceled)]_vars
FatalErrorEventuallyJoins == st.groupCanceled ~> (st.phase="returned")
ParentCancelPreservesProgress == [][Cancel => UNCHANGED <<st.ds,st.dn,st.req,st.resp,st.applied>>]_vars
JoinedShutdownIsFinal == [][(st.canceled /\ st.phase="returned") => UNCHANGED <<st.ds,st.dn,st.req,st.resp,st.applied>>]_vars
CompletionBeforeNotification == st.resp <= st.dn

\* Separate per-table configuration: one shared permit, disjoint progress.
TableInit == st=[completed |-> [t \in {"a","b","c"} |-> 0], applied |-> [t \in {"a","b","c"} |-> 0], holder |-> "none"]
AcquireTable(t) == /\ st.holder="none" /\ st.completed[t]<Batches
 /\ st'=[st EXCEPT !.holder=t]
ApplyTable(t) == /\ st.holder=t /\ st.applied[t]=st.completed[t]
 /\ st'=[st EXCEPT !.applied[t]=@+1]
CommitTable(t) == /\ st.holder=t /\ st.applied[t]>st.completed[t]
 /\ st'=[st EXCEPT !.completed[t]=st.applied[t], !.holder="none"]
TableDone == /\ (\A t \in {"a","b","c"}:st.completed[t]=Batches) /\ UNCHANGED st
TableNext == (\E t \in {"a","b","c"}:AcquireTable(t) \/ ApplyTable(t) \/ CommitTable(t)) \/ TableDone
TableSpec == TableInit /\ [][TableNext]_vars
 /\ (\A t \in {"a","b","c"}:SF_vars(AcquireTable(t)) /\ WF_vars(ApplyTable(t)) /\ WF_vars(CommitTable(t)))
TableAckMatchesOwnWork == \A t \in {"a","b","c"}:st.completed[t]<=st.applied[t]
TableCommitIsIsolated == [][\A t \in {"a","b","c"}:CommitTable(t) => (\A other \in {"a","b","c"}\{t}: st'.completed[other]=st.completed[other])]_vars
EveryTableCompletesWithFairAdmission == <>(\A t \in {"a","b","c"}:st.completed[t]=Batches)

CommittedProgressEventuallyVisible == \A b \in 1..Batches:(st.ds >= b /\ st.dn < b) ~> (st.req >= b \/ st.dn >= b \/ st.canceled)
MonotoneWithinAttempt == [][(~Restart) => (st'.req >= st.req /\ st'.resp >= st.resp)]_vars

AcknowledgmentRequiresReadySync == st.resp <= st.req /\ st.resp <= st.ds
 /\ (1..st.resp) \X (1..Files) \subseteq st.applied
AcknowledgmentWithinTarget == [][ReportNorm => st'.resp <= st.target]_vars
BackpressureStopsFurtherPulls == [][BeginPull => st.req-st.resp<Limit]_vars
DurableProgressMonotone == [][st'.ds>=st.ds /\ st'.dn>=st.dn]_vars
\* Check submitted values, even where SQL uses GREATEST.
DurableWriteArgumentsOrdered == [][(CommitSync => st.batch>=st.ds) /\ (CommitNorm => st.target>=st.dn) /\ (EmptyPoll => (EmptyPollArgument=0 /\ st'.ds=st.ds))]_vars
=============================================================================
