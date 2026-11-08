----------------------------- MODULE CDCDestinations -----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Batches, Limit, Tables, MaxFailures, MaxRestarts, Destination, GroupBatches
VARIABLE st
vars == <<st>>
Init == \E synced \in 0..Batches: \E normalized \in 0..synced:
 st = [ds |-> synced, dn |-> normalized, req |-> synced, resp |-> normalized,
       applied |-> [t \in Tables |-> normalized], schema |-> 1..synced, destinationSchema |-> 1..synced, raw |-> 1..synced, transactionRaw |-> {}, pending |-> {},
       firstBatch |-> TRUE, sp |-> "pull", np |-> "check", target |-> normalized, heartbeat |-> normalized,
       end |-> normalized, rg |-> 0, ng |-> 0, rw |-> 0, nw |-> 0, rc |-> {}, nc |-> {}, sold |-> 0, nold |-> 0, nreturn |-> "check",
       failures |-> 0, restarts |-> 0, canceled |-> FALSE]
Sync == /\ ~st.canceled /\ st.sp="pull" /\ st.ds<Batches
 /\ st'=[st EXCEPT !.sp="stage"]
Stage == /\ ~st.canceled /\ st.sp="stage"
 /\ st'=IF Destination="PostgreSQL"
 THEN [st EXCEPT !.transactionRaw={st.ds+1}, !.sp="checkpoint"]
 ELSE [st EXCEPT !.raw=@ \cup {st.ds+1}, !.sp="ddl"]
SaveSyncCheckpoint == /\ ~st.canceled /\ st.sp="checkpoint"
 /\ st'=IF Destination="PostgreSQL"
 THEN [st EXCEPT !.raw=@ \cup st.transactionRaw, !.transactionRaw={}, !.ds=@+1, !.sp="ddl"]
 ELSE [st EXCEPT !.ds=@+1, !.sp="schema"]
DestinationDDL == /\ ~st.canceled /\ st.sp="ddl"
 /\ st'=IF Destination="PostgreSQL"
 THEN [st EXCEPT !.destinationSchema=@ \cup {st.ds}, !.sp="schema"]
 ELSE [st EXCEPT !.destinationSchema=@ \cup {st.ds+1}, !.sp="checkpoint"]
Schema == /\ ~st.canceled /\ st.sp = "schema"
          /\ st' = [st EXCEPT !.schema = @ \cup {st.ds}, !.sp = "publish"]
PublishSync == /\ ~st.canceled /\ st.sp = "publish"
               /\ st' = [st EXCEPT !.firstBatch=FALSE, !.req = st.ds, !.sp = "publishSwap"]
CheckPressure == /\ ~st.canceled /\ st.sp = "check"
                 /\ st' = [st EXCEPT !.sp = IF st.resp <= st.ds-Limit THEN "subscribe" ELSE "pull"]
SubscribeSync == /\ ~st.canceled /\ st.sp = "subscribe"
                 /\ st' = [st EXCEPT !.nw = st.ng, !.sp = "sleep"]
WakeSync == /\ ~st.canceled /\ st.sp = "sleep" /\ st.nw \in st.nc
            /\ st' = [st EXCEPT !.sp = "check"]
\* ClickHouse returns one bounded group; PostgreSQL commits individual batches internally.
GroupEnd(done,target) == IF done>=target THEN target
 ELSE IF Destination="PostgreSQL" THEN done+1
 ELSE IF done+GroupBatches<target THEN done+GroupBatches ELSE target
CheckWork == /\ ~st.canceled /\ st.np = "check"
             /\ st' = IF st.req <= st.heartbeat THEN [st EXCEPT !.np = "subscribe"]
                       ELSE [st EXCEPT !.target = st.req, !.heartbeat = st.req,
                             !.end = GroupEnd(st.dn,st.req), !.np = "apply"]
SubscribeNorm == /\ ~st.canceled /\ st.np = "subscribe"
                 /\ st' = [st EXCEPT !.rw = st.rg, !.np = "sleep"]
WakeNorm == /\ ~st.canceled /\ st.np = "sleep" /\ st.rw \in st.rc
            /\ st' = [st EXCEPT !.np = "check"]
Apply(t) == /\ ~st.canceled /\ st.np = "apply"
 /\ IF Destination="PostgreSQL"
 THEN /\ t \notin st.pending /\ st'=[st EXCEPT !.pending=@ \cup {t}]
 ELSE /\ st.applied[t]<st.end /\ st'=[st EXCEPT !.applied[t]=st.end]
CommitNorm == /\ ~st.canceled /\ st.np = "apply"
 /\ IF Destination="PostgreSQL"
 THEN /\ st.pending=Tables
      /\ st'=[st EXCEPT !.applied=[t \in Tables |-> st.end], !.dn=st.end, !.pending={},
        !.np=IF st.end<st.target THEN "apply" ELSE "report",
        !.end=IF st.end<st.target THEN @+1 ELSE @]
 ELSE /\ \A t \in Tables:st.applied[t]>=st.end
      /\ st'=[st EXCEPT !.dn=st.end, !.np="report"]
ReportNorm == /\ ~st.canceled /\ st.np = "report"
              /\ st' = [st EXCEPT !.resp = st.dn, !.np = "responseSwap",
                        !.nreturn = IF st.dn < st.target THEN "apply" ELSE "check",
                        !.end = GroupEnd(st.dn,st.target)]
FailNorm == /\ ~st.canceled /\ st.np \in {"apply", "report"} /\ st.failures < MaxFailures
            /\ st' = [st EXCEPT !.failures = @+1, !.pending={}, !.np = "retry"]
RetryNorm == /\ ~st.canceled /\ st.np = "retry"
             /\ st' = [st EXCEPT !.target = st.req, !.heartbeat = st.req,
                       !.end = GroupEnd(st.dn,st.req), !.np = "apply"]
Restart == /\ ~st.canceled /\ st.restarts < MaxRestarts
 /\ st' = [st EXCEPT !.req = st.ds, !.resp = st.dn, !.heartbeat = st.dn,
 !.target = st.dn, !.end = st.dn, !.transactionRaw={}, !.pending={}, !.firstBatch=TRUE, !.sp = "pull", !.np = "check",
 !.rg = 0, !.ng = 0, !.rw = 0, !.nw = 0, !.rc = {}, !.nc = {},
 !.sold = 0, !.nold = 0, !.nreturn = "check", !.restarts = @+1]
Cancel == /\ ~st.canceled /\ st' = [st EXCEPT !.canceled = TRUE]
StopSync == /\ st.canceled /\ st.sp # "stopped" /\ st' = [st EXCEPT !.sp = "stopped"]
StopNorm == /\ st.canceled /\ st.np # "stopped" /\ st' = [st EXCEPT !.np = "stopped", !.pending={}]
Idle == /\ ((st.ds = Batches /\ st.resp = Batches /\ st.sp = "pull")
             \/ (st.sp = "stopped" /\ st.np = "stopped")) /\ UNCHANGED st
SyncSwap == /\ ~st.canceled /\ st.sp = "publishSwap"
 /\ st' = [st EXCEPT !.sold = st.rg, !.rg = @+1, !.sp = "publishClose"]
SyncClose == /\ ~st.canceled /\ st.sp = "publishClose"
 /\ st' = [st EXCEPT !.rc = @ \cup {st.sold}, !.sp = "check"]
ResponseSwap == /\ ~st.canceled /\ st.np = "responseSwap"
 /\ st' = [st EXCEPT !.nold = st.ng, !.ng = @+1, !.np = "responseClose"]
ResponseClose == /\ ~st.canceled /\ st.np = "responseClose"
 /\ st' = [st EXCEPT !.nc = @ \cup {st.nold}, !.np = st.nreturn]
Next == Stage \/ SaveSyncCheckpoint \/ DestinationDDL \/ SyncSwap \/ SyncClose \/ ResponseSwap \/ ResponseClose \/  Sync \/ Schema \/ PublishSync \/ CheckPressure \/ SubscribeSync \/ WakeSync
        \/ CheckWork \/ SubscribeNorm \/ WakeNorm \/ (\E t \in Tables: Apply(t))
        \/ CommitNorm \/ ReportNorm \/ FailNorm \/ RetryNorm \/ Restart \/ Cancel \/ StopSync \/ StopNorm \/ Idle
Spec == Init /\ [][Next]_vars
 /\ WF_vars(SyncSwap) /\ WF_vars(SyncClose) /\ WF_vars(ResponseSwap) /\ WF_vars(ResponseClose)
 /\ WF_vars(Stage) /\ WF_vars(SaveSyncCheckpoint) /\ WF_vars(DestinationDDL)
 /\ WF_vars(Sync) /\ WF_vars(Schema) /\ WF_vars(PublishSync) /\ WF_vars(CheckPressure)
 /\ WF_vars(SubscribeSync) /\ WF_vars(WakeSync) /\ WF_vars(CheckWork)
 /\ WF_vars(SubscribeNorm) /\ WF_vars(WakeNorm) /\ (\A t \in Tables: WF_vars(Apply(t)))
 /\ WF_vars(CommitNorm) /\ WF_vars(ReportNorm) /\ WF_vars(RetryNorm)
 /\ WF_vars(StopSync) /\ WF_vars(StopNorm)
NoSkippedAcknowledgment == \A t \in Tables: st.resp <= st.applied[t] /\ st.dn <= st.applied[t]
OnlyCatalogReadyWork == 1..st.req \subseteq st.schema
OnlyDestinationReadyWork == 1..st.req \subseteq st.destinationSchema
OnlyReadyWork == OnlyCatalogReadyWork /\ OnlyDestinationReadyWork
SavedCheckpointHasRaw == 1..st.ds \subseteq st.raw
PublishedWorkHasRaw == 1..st.req \subseteq st.raw
CatalogFollowsDestination == st.schema \subseteq st.destinationSchema
DestinationReadyBeforeSavedCheckpoint == 1..st.ds \subseteq st.destinationSchema
ProgressOrdered == st.resp <= st.dn /\ st.dn <= st.req /\ st.req <= st.ds
NoStrandedNormalize == ~(~st.canceled /\ st.sp = "pull" /\ st.ds = Batches
                         /\ st.np = "sleep" /\ st.rw \notin st.rc /\ st.req > st.resp)
NoStrandedSync == ~(~st.canceled /\ st.sp = "sleep" /\ st.nw \notin st.nc
                    /\ st.np = "sleep" /\ st.rw \notin st.rc /\ st.resp = st.req)
AllPublishedComplete == \A b \in 1..Batches: (st.req >= b) ~> (st.resp >= b \/ st.canceled)
ShutdownCompletes == st.canceled ~> (st.sp = "stopped" /\ st.np = "stopped")

BoundaryExact == [][CheckPressure =>
 (st'.sp = "subscribe" <=> st.ds-st.resp >= Limit)]_vars
FailureDoesNotAcknowledge == [][FailNorm => UNCHANGED <<st.dn,st.resp>>]_vars
RetryUsesLatest == [][RetryNorm => st'.target = st.req]_vars
TargetsDoNotAcknowledge == [][CheckWork => UNCHANGED <<st.dn,st.resp>>]_vars
CompletionBeforeNotification == st.resp <= st.dn

\* Isolated configuration of the actual ClickHouse NormalizeRecords worker lifecycle.
WorkerInit == st=[sent |-> {}, active |-> {}, applied |-> {}, returned |-> FALSE, retried |-> FALSE, ack |-> FALSE, failed |-> FALSE]
Dispatch(t) == /\ ~st.returned /\ ~st.failed /\ t \notin st.sent
 /\ st'=[st EXCEPT !.sent=@ \cup {t}, !.active=@ \cup {t}]
ProducerError == /\ ~st.returned /\ st.sent # Tables
 /\ st'=[st EXCEPT !.returned=TRUE]
WorkerFinish(t) == /\ t \in st.active
 /\ st'=[st EXCEPT !.active=@ \ {t}, !.applied=@ \cup {t}]
WorkerTaskError(t) == /\ t \in st.active
 /\ st'=[st EXCEPT !.active=@ \ {t}, !.failed=TRUE]
JoinedError == /\ ~st.returned /\ st.failed /\ st.active={}
 /\ st'=[st EXCEPT !.returned=TRUE]
SuccessfulJoin == /\ ~st.failed /\ st.sent=Tables /\ st.active={} /\ ~st.returned
 /\ st'=[st EXCEPT !.returned=TRUE, !.ack=TRUE]
CallerRetries == /\ st.returned /\ ~st.ack /\ ~st.retried
 /\ st'=[st EXCEPT !.retried=TRUE]
WorkerDone == /\ st.returned /\ st.active={} /\ UNCHANGED st
WorkerNext == (\E t \in Tables: Dispatch(t)) \/ ProducerError \/ (\E t \in Tables: WorkerFinish(t) \/ WorkerTaskError(t)) \/ JoinedError \/ SuccessfulJoin \/ CallerRetries \/ WorkerDone
WorkerSpec == WorkerInit /\ [][WorkerNext]_vars /\ (\A t \in Tables: WF_vars(WorkerFinish(t)))
WorkersJoinedBeforeReturn == st.returned => st.active={}
RetryHasNoPriorWorkers == st.retried => st.active={}
AllTablesBeforeAck == st.ack => st.applied=Tables


AcknowledgmentRequiresReadySync == st.resp <= st.ds /\ 1..st.resp \subseteq st.schema /\ 1..st.resp \subseteq st.destinationSchema /\ 1..st.resp \subseteq st.raw
AcknowledgmentWithinTarget == [][ReportNorm => st'.resp <= st.target]_vars
BackpressureStopsFurtherPulls == [][Sync => (st.firstBatch \/ st.ds-st.resp<Limit)]_vars
DurableProgressMonotone == [][st'.ds>=st.ds /\ st'.dn>=st.dn]_vars
DurableWriteArgumentsOrdered == [][CommitNorm => st.end>=st.dn]_vars
\* PostgreSQL commits batches internally but returns only the captured target.
PostgresReportsWholeCall == [][(Destination="PostgreSQL" /\ ReportNorm) => st.dn=st.target]_vars
NoPublicationDuringInternalCommit == [][CommitNorm => UNCHANGED st.resp]_vars
PostgresEffectsAndCheckpointAtomic == Destination # "PostgreSQL" \/ (\A t \in Tables:st.applied[t]=st.dn)
=============================================================================
