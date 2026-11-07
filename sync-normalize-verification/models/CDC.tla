----------------------------- MODULE CDC -----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Batches, Limit, Tables, MaxFailures, MaxRestarts, MaxSyncFailures, Destination, GroupBatches, Workers, PrimaryWorker
VARIABLE st
vars == <<st>>
Init == \E synced \in 0..Batches: \E normalized \in 0..synced:
 st = [available |-> synced, ds |-> synced, dn |-> normalized, req |-> synced, resp |-> normalized,
       applied |-> [t \in Tables |-> normalized], schema |-> 1..synced, destinationSchema |-> 1..synced, raw |-> 1..synced, transactionRaw |-> {},
       firstBatch |-> TRUE, sp |-> "pull", np |-> "check", target |-> normalized, heartbeat |-> normalized,
       pending |-> {}, callEnd |-> normalized, end |-> normalized, g |-> 0, rw |-> 0, nw |-> 0, rc |-> {}, nc |-> {}, sold |-> 0, nold |-> 0, nreturn |-> "check",
       normCanceled |-> FALSE, syncFailures |-> 0, failures |-> 0, restarts |-> 0, canceled |-> FALSE]
Arrival == /\ st.available < Batches /\ st'=[st EXCEPT !.available=@+1]
\* Arrival is deliberately not fair: the source may stay quiet at any prefix.
Sync == /\ ~st.canceled /\ st.sp="pull" /\ st.ds<st.available
 /\ st'=[st EXCEPT !.sp="stage"]
\* PR 1 snapshot e27a7f5f23f6515bcf5e289f22113fed38473e3e.
\* PostgreSQL syncRecordsCore: raw/checkpoint commit precedes destination DDL.
\* ClickHouse SyncRecords: staging and destination DDL precede FinishBatch.
\* Both: pullAndSyncCore applies catalog schema after the connector returns.
Stage == /\ st.sp="stage"
 /\ st'=IF Destination="PostgreSQL"
 THEN [st EXCEPT !.transactionRaw={st.ds+1}, !.sp="checkpoint"]
 ELSE [st EXCEPT !.raw=@ \cup {st.ds+1}, !.sp="ddl"]
DestinationDDL == /\ st.sp="ddl"
 /\ st'=IF Destination="PostgreSQL"
 THEN [st EXCEPT !.destinationSchema=@ \cup {st.ds}, !.sp="schema"]
 ELSE [st EXCEPT !.destinationSchema=@ \cup {st.ds+1}, !.sp="checkpoint"]
SaveSyncCheckpoint == /\ st.sp="checkpoint"
 /\ st'=IF Destination="PostgreSQL"
 THEN [st EXCEPT !.raw=@ \cup st.transactionRaw, !.transactionRaw={}, !.ds=@+1, !.sp="ddl"]
 ELSE [st EXCEPT !.ds=@+1, !.sp="schema"]
\* DDL is a pre-checkpoint boundary only for ClickHouse in PR 1.
FailSyncBeforeCommit == /\ ~st.canceled
 /\ (st.sp \in {"stage","checkpoint"} \/ (Destination="ClickHouse" /\ st.sp="ddl"))
 /\ st.syncFailures<MaxSyncFailures
 /\ st'=[st EXCEPT !.transactionRaw={}, !.sp="failed", !.syncFailures=@+1]
Schema == /\ st.sp = "schema"
          /\ st' = [st EXCEPT !.schema = @ \cup {st.ds}, !.sp = "publish"]
PublishSync == /\ st.sp = "publish"
 /\ st' = [st EXCEPT !.firstBatch=FALSE, !.req = st.ds, !.g = IF st.ds > st.req THEN @+1 ELSE @, !.sp = "check"]
CheckPressure == /\ ~st.canceled /\ st.sp = "check"
 /\ st' = [st EXCEPT !.sp = IF st.req-st.resp >= Limit THEN "sleep" ELSE "pull", !.nw=st.g]
WakeSync == /\ ~st.canceled /\ st.sp = "sleep" /\ st.nw < st.g
            /\ st' = [st EXCEPT !.sp = "check"]
CallEnd(dn,target) == IF Destination="PostgreSQL" THEN target
 ELSE IF dn+GroupBatches < target THEN dn+GroupBatches ELSE target
BeginCall(target) == [st EXCEPT !.target=target, !.heartbeat=target,
 !.callEnd=CallEnd(st.dn,target), !.pending={},
 !.end=IF Destination="PostgreSQL" /\ st.dn<target THEN st.dn+1 ELSE CallEnd(st.dn,target),
 !.np=IF st.dn>=target THEN "returned" ELSE IF Destination="ClickHouse" THEN "load" ELSE "apply"]
CheckWork == /\ ~st.canceled /\ ~st.normCanceled /\ st.np = "check"
 /\ st' = IF st.req <= st.resp THEN [st EXCEPT !.rw=st.g, !.np="sleep"] ELSE BeginCall(st.req)
WakeNorm == /\ ~st.canceled /\ st.np = "sleep" /\ st.rw < st.g
 /\ st' = [st EXCEPT !.np = "check"]
\* Successful stage loading is abstracted; raw means durable Avro/stage availability for CH.
LoadStages == /\ st.np="load" /\ st'=[st EXCEPT !.np="apply"]
Apply(t) == /\ st.np = "apply"
 /\ IF Destination="PostgreSQL"
     THEN /\ t \notin st.pending /\ st'=[st EXCEPT !.pending=@ \cup {t}]
     ELSE /\ st.applied[t]<st.end /\ st'=[st EXCEPT !.applied[t]=st.end]
CommitNorm == /\ st.np="apply"
 /\ IF Destination="PostgreSQL"
 THEN /\ st.pending=Tables
      /\ st'=[st EXCEPT !.applied=[t \in Tables |-> st.end], !.dn=st.end, !.pending={},
       !.np=IF st.end<st.callEnd THEN "apply" ELSE "returned",
       !.end=IF st.end<st.callEnd THEN @+1 ELSE @]
 ELSE /\ \A t \in Tables:st.applied[t]>=st.end
      /\ st'=[st EXCEPT !.dn=st.end, !.np="returned"]
ConnectorReturned == /\ st.np="returned"
 /\ st'=[st EXCEPT !.np=IF Destination="PostgreSQL" THEN "bookkeeping" ELSE "report"]
Bookkeeping == /\ st.np="bookkeeping" /\ st'=[st EXCEPT !.np="report"]
ReportNorm == /\ st.np="report"
 /\ st'=[st EXCEPT !.resp=st.callEnd, !.g=IF st.callEnd>st.resp THEN @+1 ELSE @,
 !.np=IF st.callEnd<st.target THEN "nextCall" ELSE "check"]
NextCall == /\ ~st.canceled /\ ~st.normCanceled /\ st.np="nextCall" /\ st'=BeginCall(st.target)
FailNorm == /\ ~st.canceled /\ st.np \in {"load","apply","returned","bookkeeping"}
 /\ st.failures<MaxFailures
 /\ st'=[st EXCEPT !.failures=@+1, !.pending={}, !.np="retry"]
RetryNorm == /\ ~st.canceled /\ ~st.normCanceled /\ st.np="retry" /\ st'=BeginCall(st.req)
\* All sync errors leave the sync loop, including errors after a durable checkpoint.
FailAfterSyncCommit == /\ ~st.canceled
 /\ (st.sp \in {"schema","publish"} \/ (Destination="PostgreSQL" /\ st.sp="ddl"))
 /\ st.syncFailures<MaxSyncFailures
 /\ st'=[st EXCEPT !.sp="failed", !.syncFailures=@+1]
CancelNormalize == /\ ~st.canceled /\ st.sp="failed"
 /\ st'=[st EXCEPT !.normCanceled=TRUE, !.sp="joining"]
\* Internal operations may finish during cancellation; StopNorm is the joined exit.
JoinNormalize == /\ ~st.canceled /\ st.sp="joining" /\ st.np="stopped"
 /\ st'=[st EXCEPT !.sp="returned"]
\* Reserve recovery budget for each still-possible sync failure. Extra budget
\* permits independent process loss; terminated goroutines cannot survive it.
CrashActivity == /\ ~st.canceled /\ st.sp \notin {"failed","joining","returned","stopped"}
 /\ st.restarts+(MaxSyncFailures-st.syncFailures)<MaxRestarts
 /\ st'=[st EXCEPT !.sp="returned", !.np="stopped", !.normCanceled=TRUE,
 !.pending={}, !.transactionRaw={}]
Restart == /\ ~st.canceled /\ st.sp="returned" /\ st.np="stopped"
 /\ st.restarts < MaxRestarts
 /\ st' = [st EXCEPT !.req = st.ds, !.resp = st.dn, !.heartbeat = st.dn,
 !.target = st.dn, !.end = st.dn, !.callEnd=st.dn, !.pending={}, !.transactionRaw={}, !.firstBatch=TRUE, !.sp = "pull", !.np = "check",
 !.g = 0, !.rw = 0, !.nw = 0, !.rc = {}, !.nc = {}, !.normCanceled=FALSE,
 !.sold = 0, !.nold = 0, !.nreturn = "check", !.restarts = @+1]
Cancel == /\ ~st.canceled /\ st' = [st EXCEPT !.canceled = TRUE]
StopSync == /\ st.canceled /\ st.np="stopped" /\ st.sp \notin {"stopped","publish"} /\ st' = [st EXCEPT !.sp = "stopped"]
StopNorm == /\ (st.canceled \/ st.normCanceled) /\ st.np \notin {"stopped","report"} /\ st' = [st EXCEPT !.np = "stopped", !.pending={}]
Idle == /\ ((st.ds = st.available /\ st.resp = st.ds /\ st.sp = "pull")
             \/ (st.sp = "stopped" /\ st.np = "stopped")) /\ UNCHANGED st




Next == Arrival \/ FailAfterSyncCommit \/ CancelNormalize \/ JoinNormalize \/ CrashActivity \/ Stage \/ DestinationDDL \/ SaveSyncCheckpoint \/ FailSyncBeforeCommit \/ Sync \/ Schema \/ PublishSync \/ CheckPressure \/ WakeSync
        \/ CheckWork \/ WakeNorm \/ (\E t \in Tables: Apply(t))
        \/ LoadStages \/ CommitNorm \/ ConnectorReturned \/ Bookkeeping \/ NextCall \/ ReportNorm \/ FailNorm \/ RetryNorm \/ Restart \/ Cancel \/ StopSync \/ StopNorm \/ Idle
Spec == Init /\ [][Next]_vars

 /\ WF_vars(LoadStages) /\ WF_vars(ConnectorReturned) /\ WF_vars(Bookkeeping) /\ WF_vars(NextCall) /\ WF_vars(CancelNormalize) /\ WF_vars(JoinNormalize) /\ WF_vars(Restart)
 /\ WF_vars(Stage) /\ WF_vars(DestinationDDL) /\ WF_vars(SaveSyncCheckpoint)
 /\ WF_vars(Sync) /\ WF_vars(Schema) /\ WF_vars(PublishSync) /\ WF_vars(CheckPressure)
 /\ WF_vars(WakeSync) /\ WF_vars(CheckWork)
 /\ WF_vars(WakeNorm) /\ (\A t \in Tables: WF_vars(Apply(t)))
 /\ WF_vars(CommitNorm) /\ WF_vars(ReportNorm) /\ WF_vars(RetryNorm)
 /\ WF_vars(StopSync) /\ WF_vars(StopNorm)
NoSkippedAcknowledgment == \A t \in Tables: st.resp <= st.applied[t] /\ st.dn <= st.applied[t]
OnlyCatalogReadyWork == 1..st.req \subseteq st.schema
OnlyDestinationReadyWork == 1..st.req \subseteq st.destinationSchema
OnlyReadyWork == OnlyCatalogReadyWork /\ OnlyDestinationReadyWork
\* Separate checks expose the two PostgreSQL gaps and the ClickHouse catalog gap.
SavedCheckpointHasCatalogSchema == 1..st.ds \subseteq st.schema
SavedCheckpointHasDestinationSchema == 1..st.ds \subseteq st.destinationSchema
SavedCheckpointIsReady == (1..st.ds \subseteq st.schema) /\ (1..st.ds \subseteq st.destinationSchema)
SavedCheckpointHasRaw == 1..st.ds \subseteq st.raw
CatalogFollowsDestination == st.schema \subseteq st.destinationSchema
SyncFailureDoesNotPublish == [][FailSyncBeforeCommit => UNCHANGED <<st.ds,st.req>>]_vars
SyncErrorsLeaveActivity == [][(FailSyncBeforeCommit \/ FailAfterSyncCommit) => st'.sp="failed"]_vars
ReturnedActivityHasNoNormalizer == st.sp="returned" => st.np="stopped"
NoSyncDuringJoin == st.normCanceled => st.sp \in {"joining","returned","stopped"}
RestartSeedsDurableProgress == [][Restart => (st'.req=st.ds /\ st'.resp=st.dn /\ st'.g=0 /\ ~st'.normCanceled)]_vars
RestartRequiresNormalizerExit == [][Restart => (st.sp="returned" /\ st.np="stopped") ]_vars
SyncErrorEventuallyReturns == st.sp="failed" ~> (st.sp="returned" \/ st.canceled)
ProgressOrdered == st.resp <= st.dn /\ st.dn <= st.req /\ st.req <= st.ds
NoStrandedNormalize == ~(~st.canceled /\ st.sp = "pull" /\ st.ds = st.available
                         /\ st.np = "sleep" /\ st.rw = st.g /\ st.req > st.resp)
NoStrandedSync == ~(~st.canceled /\ st.sp = "sleep" /\ st.nw = st.g
                    /\ st.np = "sleep" /\ st.rw = st.g /\ st.resp = st.req)
AllPublishedComplete == \A b \in 1..Batches: (st.req >= b) ~> (st.resp >= b \/ st.canceled)
ShutdownCompletes == st.canceled ~> (st.sp = "stopped" /\ st.np = "stopped")

BoundaryExact == [][CheckPressure =>
 (st'.sp = "sleep" <=> st.req-st.resp >= Limit)]_vars
FailureDoesNotAcknowledge == [][FailNorm => UNCHANGED <<st.dn,st.resp>>]_vars
RetryUsesLatest == [][RetryNorm => st'.target = st.req]_vars
TargetsDoNotAcknowledge == [][CheckWork => UNCHANGED <<st.dn,st.resp>>]_vars
PostgresReportsWholeCall == [][(Destination="PostgreSQL" /\ ReportNorm) => st.dn=st.target]_vars
PostgresEffectsAndCheckpointAtomic == Destination="PostgreSQL" => (\A t \in Tables:st.applied[t]=st.dn)
ReportMatchesCallBoundary == [][ReportNorm => st'.resp=st.callEnd]_vars
NoPublicationDuringInternalCommit == [][CommitNorm => UNCHANGED st.resp]_vars
ReadyCompletionCanPublish == st.np="report" => ENABLED ReportNorm
ReadySyncCanPublish == st.sp="publish" => ENABLED PublishSync
ParentCancelPreservesProgress == [][Cancel => UNCHANGED <<st.ds,st.dn,st.req,st.resp,st.applied>>]_vars
JoinedShutdownIsFinal == [][(st.canceled /\ st.sp="stopped" /\ st.np="stopped") => UNCHANGED <<st.ds,st.dn,st.req,st.resp,st.applied>>]_vars
CompletionBeforeNotification == st.resp <= st.dn

\* Separate identities for pool goroutines and tables; one modeled call plus retry boundary.
WorkerInit == \E done \in SUBSET Tables:
 st=[sent |-> {}, skipped |-> {}, busy |-> [w \in Workers |-> "none"],
 pendingError |-> {}, phase |-> [w \in Workers |-> IF w=PrimaryWorker THEN "idle" ELSE "connect"], alive |-> Workers, applied |-> done, watermark |-> done,
 producerDone |-> FALSE, taskError |-> FALSE, parentCanceled |-> FALSE,
 joined |-> FALSE, returned |-> FALSE, retried |-> FALSE, globalSaved |-> FALSE,
 result |-> "pending"]
ErrCtxCanceled == st.parentCanceled \/ st.taskError \/ st.joined
Dispatch(w,t) == /\ ~st.producerDone /\ w \in st.alive /\ st.phase[w]="idle"
 /\ t \notin (st.sent \cup st.skipped) /\ t \notin st.watermark
 /\ st'=[st EXCEPT !.sent=@ \cup {t}, !.busy[w]=t, !.phase[w]="insert"]
SkipTable(t) == /\ ~st.producerDone /\ t \in st.watermark /\ t \notin (st.sent \cup st.skipped)
 /\ st'=[st EXCEPT !.skipped=@ \cup {t}]
ProducerError == /\ ~st.producerDone /\ st'=[st EXCEPT !.producerDone=TRUE, !.taskError=TRUE]
ProducerFinish == /\ ~st.producerDone /\ st.sent \cup st.skipped=Tables
 /\ st'=[st EXCEPT !.producerDone=TRUE]
ProducerObservesCancellation == /\ ~st.producerDone /\ ErrCtxCanceled
 /\ st'=[st EXCEPT !.producerDone=TRUE, !.taskError=TRUE]
InsertSuccess(w) == /\ st.phase[w]="insert"
 /\ st'=[st EXCEPT !.applied=@ \cup {st.busy[w]}, !.phase[w]="watermark"]
InsertError(w) == /\ st.phase[w]="insert"
 /\ st'=[st EXCEPT !.phase[w]="cleanup", !.busy[w]="none", !.pendingError=@ \cup {w}]
\* A remote INSERT may commit before the driver reports an error.
InsertCommitError(w) == /\ st.phase[w]="insert"
 /\ st'=[st EXCEPT !.applied=@ \cup {st.busy[w]}, !.phase[w]="cleanup", !.busy[w]="none", !.pendingError=@ \cup {w}]
TableWatermarkSuccess(w) == /\ st.phase[w]="watermark"
 /\ st'=[st EXCEPT !.watermark=@ \cup {st.busy[w]}, !.busy[w]="none", !.phase[w]="idle"]
TableWatermarkError(w) == /\ st.phase[w]="watermark"
 /\ st'=[st EXCEPT !.phase[w]="cleanup", !.busy[w]="none", !.pendingError=@ \cup {w}]
TableWatermarkCommitError(w) == /\ st.phase[w]="watermark"
 /\ st'=[st EXCEPT !.watermark=@ \cup {st.busy[w]}, !.phase[w]="cleanup", !.busy[w]="none", !.pendingError=@ \cup {w}]
ConnectionSuccess(w) == /\ st.phase[w]="connect" /\ st'= [st EXCEPT !.phase[w]="idle"]
ConnectionError(w) == /\ st.phase[w]="connect"
 /\ st'=[st EXCEPT !.phase[w]="cleanup", !.pendingError=@ \cup {w}]
WorkerExitLoop(w) == /\ st.phase[w]="idle" /\ st.producerDone
 /\ st'=[st EXCEPT !.phase[w]="cleanup"]
\* Deferred connection Close is inside the joined goroutine.
CleanupDone(w) == /\ st.phase[w]="cleanup"
 /\ st'=[st EXCEPT !.phase[w]="exited", !.alive=@ \ {w}, !.taskError=@ \/ (w \in st.pendingError)]
WorkerAdvance(w) == ConnectionSuccess(w) \/ InsertSuccess(w) \/ TableWatermarkSuccess(w) \/ WorkerExitLoop(w) \/ CleanupDone(w)
ParentCancel == /\ ~st.parentCanceled /\ ~st.returned
 /\ st'=[st EXCEPT !.parentCanceled=TRUE]
Join == /\ st.producerDone /\ st.alive={} /\ ~st.joined
 /\ st'=[st EXCEPT !.joined=TRUE, !.result=IF st.taskError THEN "error" ELSE "metadata"]
\* Global metadata uses parent ctx, not errCtx (Wait cancels errCtx even on success).
GlobalCommit == /\ st.joined /\ st.result="metadata"
 /\ st'=[st EXCEPT !.globalSaved=TRUE, !.result="success"]
GlobalError == /\ st.joined /\ st.result="metadata"
 /\ st'=[st EXCEPT !.result="error"]
GlobalCommitError == /\ st.joined /\ st.result="metadata"
 /\ st'=[st EXCEPT !.globalSaved=TRUE, !.result="error"]
Return == /\ st.result \in {"success","error"} /\ ~st.returned
 /\ st'=[st EXCEPT !.returned=TRUE]
CallerRetries == /\ st.returned /\ st.result="error" /\ ~st.retried
 /\ st'=[st EXCEPT !.retried=TRUE]
WorkerDone == /\ st.returned /\ UNCHANGED st
WorkerNext == (\E w \in Workers:(\E t \in Tables:Dispatch(w,t)) \/ WorkerAdvance(w) \/ InsertError(w) \/ InsertCommitError(w) \/ TableWatermarkError(w) \/ TableWatermarkCommitError(w) \/ ConnectionError(w))
 \/ (\E t \in Tables:SkipTable(t)) \/ ProducerError \/ ProducerFinish \/ ProducerObservesCancellation
 \/ ParentCancel \/ Join \/ GlobalCommit \/ GlobalError \/ GlobalCommitError \/ Return \/ CallerRetries \/ WorkerDone
WorkerSpec == WorkerInit /\ [][WorkerNext]_vars
 /\ (\A w \in Workers:WF_vars(WorkerAdvance(w)))
 /\ WF_vars((\E w \in Workers:\E t \in Tables:Dispatch(w,t)) \/ (\E t \in Tables:SkipTable(t)))
 /\ WF_vars(ProducerFinish) /\ WF_vars(ProducerObservesCancellation)
 /\ WF_vars(Join) /\ WF_vars(GlobalCommit \/ GlobalError \/ GlobalCommitError) /\ WF_vars(Return)
WorkersJoinedBeforeReturn == st.returned => (st.alive={} /\ st.producerDone /\ st.joined)
RetryHasNoPriorWorkers == st.retried => st.alive={}
AllTablesBeforeAck == st.globalSaved => st.applied=Tables /\ st.watermark=Tables
\* Task errors prevent the global write; parent cancellation alone is not an errgroup error.
NoAckOnFailure == st.taskError => ~st.globalSaved
WatermarksRequireEffects == st.watermark \subseteq st.applied
SuccessRequiresGlobalCommit == (st.returned /\ st.result="success") => st.globalSaved
MetadataOnlyAfterJoin == st.globalSaved => st.joined
FailedCallEventuallyReturns == st.taskError ~> st.returned
CallEventuallyReturns == <>st.returned

MonotoneWithinAttempt == [][(~Restart) => (st'.req >= st.req /\ st'.resp >= st.resp)]_vars

AcknowledgmentRequiresReadySync == st.resp <= st.req /\ st.resp <= st.ds
 /\ 1..st.resp \subseteq st.schema /\ 1..st.resp \subseteq st.destinationSchema
 /\ 1..st.resp \subseteq st.raw
AcknowledgmentWithinTarget == [][ReportNorm => st'.resp <= st.target]_vars
\* CDC checks after publication: allow the first batch of each activity attempt.
BackpressureStopsFurtherPulls == [][Sync => (st.firstBatch \/ st.req-st.resp<Limit)]_vars
DurableProgressMonotone == [][st'.ds>=st.ds /\ st'.dn>=st.dn]_vars
DurableWriteArgumentsOrdered == [][CommitNorm => st.end>=st.dn]_vars
=============================================================================
