----------------------------- MODULE QueryGroup -----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Batches, Limit, MaxFailures, MaxRestarts, MaxFatalFailures, Files
VARIABLES left, right, phase, canceled, fatalFailures, restarts
vars == <<left,right,phase,canceled,fatalFailures,restarts>>
A == INSTANCE QueryCDC WITH st <- left
B == INSTANCE QueryCDC WITH st <- right
Tables == {"a","b"}
Q(t) == IF t="a" THEN left ELSE right
Init == /\ left \in A!InitialStates /\ right \in B!InitialStates
 /\ phase="running" /\ canceled=FALSE /\ fatalFailures=0 /\ restarts=0
SyncAdvance(t) == /\ (IF t="a" THEN ((A!CheckPressure \/ A!WakeSync \/ A!BeginPull \/ A!Stage \/ A!FinishPull \/ A!CommitSync \/ A!PublishSync) /\ UNCHANGED right) ELSE ((B!CheckPressure \/ B!WakeSync \/ B!BeginPull \/ B!Stage \/ B!FinishPull \/ B!CommitSync \/ B!PublishSync) /\ UNCHANGED left))
 /\ UNCHANGED <<phase,canceled,fatalFailures,restarts>>
NormAdvance(t) == /\ (IF t="a" THEN ((A!CheckWork \/ A!AcquireNormConnector \/ A!WakeNorm \/ A!SkipMissing \/ (\E f \in 1..Files:A!Apply(f)) \/ A!DeleteStage \/ A!FinishRange \/ A!CommitNorm \/ A!ReportNorm) /\ UNCHANGED right) ELSE ((B!CheckWork \/ B!AcquireNormConnector \/ B!WakeNorm \/ B!SkipMissing \/ (\E f \in 1..Files:B!Apply(f)) \/ B!DeleteStage \/ B!FinishRange \/ B!CommitNorm \/ B!ReportNorm) /\ UNCHANGED left))
 /\ UNCHANGED <<phase,canceled,fatalFailures,restarts>>
OtherStep(t) == /\ (IF t="a" THEN ((A!Arrival \/ A!EmptyPoll \/ A!FailPull \/ A!FailNorm) /\ UNCHANGED right) ELSE ((B!Arrival \/ B!EmptyPoll \/ B!FailPull \/ B!FailNorm) /\ UNCHANGED left))
 /\ UNCHANGED <<phase,canceled,fatalFailures,restarts>>
ExitSync(t) == /\ (IF t="a" THEN ((A!StopSync) /\ UNCHANGED right) ELSE ((B!StopSync) /\ UNCHANGED left))
 /\ UNCHANGED <<phase,canceled,fatalFailures,restarts>>
ExitNorm(t) == /\ (IF t="a" THEN ((A!StopNorm) /\ UNCHANGED right) ELSE ((B!StopNorm) /\ UNCHANGED left))
 /\ UNCHANGED <<phase,canceled,fatalFailures,restarts>>
FatalError(t) == /\ ~canceled /\ phase="running" /\ fatalFailures<MaxFatalFailures
 /\ (IF t="a" THEN ((A!FatalSyncError \/ A!FatalNormError) /\ right'=[right EXCEPT !.groupCanceled=TRUE, !.phase="canceling"]) ELSE ((B!FatalSyncError \/ B!FatalNormError) /\ left'=[left EXCEPT !.groupCanceled=TRUE, !.phase="canceling"]))
 /\ phase'="canceling" /\ fatalFailures'=fatalFailures+1 /\ UNCHANGED <<canceled,restarts>>
ParentCancel == /\ ~canceled /\ phase # "returned"
 /\ canceled'=TRUE /\ phase'="canceling"
 /\ left'=[left EXCEPT !.canceled=TRUE, !.phase="canceling"]
 /\ right'=[right EXCEPT !.canceled=TRUE, !.phase="canceling"]
 /\ UNCHANGED <<fatalFailures,restarts>>
AllExited == \A t \in Tables:Q(t).sp="stopped" /\ Q(t).np="stopped"
JoinGroup == /\ phase="canceling" /\ AllExited /\ phase'="returned"
 /\ left'=[left EXCEPT !.phase="returned"] /\ right'=[right EXCEPT !.phase="returned"]
 /\ UNCHANGED <<canceled,fatalFailures,restarts>>
Restart == /\ ~canceled /\ phase="returned" /\ AllExited /\ restarts<MaxRestarts
 /\ left'=A!Reconstructed /\ right'=B!Reconstructed
 /\ phase'="running" /\ restarts'=restarts+1 /\ UNCHANGED <<canceled,fatalFailures>>
Idle == /\ (phase="returned" \/ (\A t \in Tables:Q(t).ds=Q(t).available /\ Q(t).dn=Q(t).ds)) /\ UNCHANGED vars
Next == (\E t \in Tables:SyncAdvance(t) \/ NormAdvance(t) \/ OtherStep(t) \/ ExitSync(t) \/ ExitNorm(t) \/ FatalError(t))
 \/ ParentCancel \/ JoinGroup \/ Restart \/ Idle
Spec == Init /\ [][Next]_vars
 /\ (\A t \in Tables:WF_vars(SyncAdvance(t)) /\ WF_vars(NormAdvance(t)) /\ WF_vars(ExitSync(t)) /\ WF_vars(ExitNorm(t)))
 /\ WF_vars(JoinGroup) /\ WF_vars(Restart)
NoSkippedAcknowledgment == A!NoSkippedAcknowledgment /\ B!NoSkippedAcknowledgment
PublishedWorkRecoverable == A!PublishedWorkRecoverable /\ B!PublishedWorkRecoverable
ProgressOrdered == A!ProgressOrdered /\ B!ProgressOrdered
ReturnedGroupHasNoLoops == phase="returned" => AllExited
ReadyPublicationsRemainEnabled == A!ReadyCompletionCanPublish /\ B!ReadyCompletionCanPublish /\ A!ReadySyncCanPublish /\ B!ReadySyncCanPublish
RestartRequiresAllLoops == [][Restart => AllExited]_vars
RestartSeedsEveryTable == [][Restart => (left'.req=left.ds /\ left'.resp=left.dn /\ right'.req=right.ds /\ right'.resp=right.dn)]_vars
FatalErrorCancelsEveryTable == [][(\E t \in Tables:FatalError(t)) => (left'.groupCanceled /\ right'.groupCanceled)]_vars
GroupErrorEventuallyJoins == phase="canceling" ~> phase="returned"
EveryCommittedBatchCompletes == \A t \in Tables:\A b \in 1..Batches:Q(t).ds>=b ~> (Q(t).dn>=b \/ canceled)
ShutdownCompletes == canceled ~> (phase="returned" /\ AllExited)

AcknowledgmentRequiresReadySync == A!AcknowledgmentRequiresReadySync /\ B!AcknowledgmentRequiresReadySync
AcknowledgmentWithinTarget == A!AcknowledgmentWithinTarget /\ B!AcknowledgmentWithinTarget
BackpressureStopsFurtherPulls == A!BackpressureStopsFurtherPulls /\ B!BackpressureStopsFurtherPulls
DurableProgressMonotone == A!DurableProgressMonotone /\ B!DurableProgressMonotone
DurableWriteArgumentsOrdered == A!DurableWriteArgumentsOrdered /\ B!DurableWriteArgumentsOrdered
=============================================================================
