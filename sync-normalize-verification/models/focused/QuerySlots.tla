----------------------------- MODULE QuerySlots -----------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Tables, Mode, Attempts, MaxFailures
VARIABLE st
vars == <<st>>
FirstWait == IF Mode="sync" THEN "backpressure" ELSE "workWait"
Init == st=[phase |-> [t \in Tables |-> FirstWait], holder |-> "none",
 attempts |-> [t \in Tables |-> 0], failures |-> [t \in Tables |-> 0],
 failed |-> [t \in Tables |-> FALSE], fatal |-> [t \in Tables |-> FALSE], groupError |-> FALSE, canceled |-> FALSE]
\* Baseline LastChan/catalog/poll wait completion abstracts the clock/work notification; no semaphore acquired yet.
WaitReady(t) == /\ ~st.canceled /\ st.phase[t]=FirstWait
 /\ st'=[st EXCEPT !.phase[t]=IF Mode="sync" THEN "pollWait" ELSE "acquire"]
PollReady(t) == /\ ~st.canceled /\ st.phase[t]="pollWait"
 /\ st'=[st EXCEPT !.phase[t]="acquire"]
\* The actual free-slot fast path can succeed after cancellation.
Acquire(t) == /\ st.phase[t]="acquire" /\ st.holder="none"
 /\ st'=[st EXCEPT !.holder=t, !.phase[t]="io"]
IOSuccess(t) == /\ st.phase[t]="io"
 /\ st'=[st EXCEPT !.failed[t]=FALSE, !.phase[t]="release"]
IOFailure(t) == /\ st.phase[t]="io" /\ st.failures[t]<MaxFailures
 /\ st'=[st EXCEPT !.failed[t]=TRUE, !.failures[t]=@+1, !.phase[t]="release"]
\* Deferred connector close and inner errgroup joins finish before release.
IOFatal(t) == /\ st.phase[t]="io" /\ ~st.canceled /\ st.failures[t]<MaxFailures
 /\ st'=[st EXCEPT !.fatal[t]=TRUE, !.failures[t]=@+1, !.phase[t]="release"]
Release(t) == /\ st.phase[t]="release" /\ st.holder=t
 /\ st'=[st EXCEPT !.holder="none", !.attempts[t]=@+1,
 !.phase[t]=IF st.fatal[t] THEN "fatalReturn" ELSE IF st.failed[t]
 THEN IF st.canceled \/ st.attempts[t]+1=Attempts THEN "stopped" ELSE "retry"
 ELSE "commit"]
Commit(t) == /\ st.phase[t]="commit"
 /\ st'=[st EXCEPT !.phase[t]=IF st.canceled \/ st.attempts[t]=Attempts THEN "stopped" ELSE FirstWait]
FatalReturn(t) == /\ st.phase[t]="fatalReturn"
 /\ st'=[st EXCEPT !.phase[t]="stopped", !.canceled=TRUE, !.groupError=TRUE]
CommitError(t) == /\ ~st.canceled /\ st.phase[t]="commit" /\ st.failures[t]<MaxFailures
 /\ st'=[st EXCEPT !.phase[t]="stopped", !.canceled=TRUE, !.groupError=TRUE, !.failures[t]=@+1]
RetryReady(t) == /\ ~st.canceled /\ st.phase[t]="retry"
 /\ st'=[st EXCEPT !.phase[t]=FirstWait]
Cancel == /\ ~st.canceled /\ st'=[st EXCEPT !.canceled=TRUE]
StopWaiting(t) == /\ st.canceled /\ st.phase[t] \notin {"io","release","stopped"}
 /\ st'=[st EXCEPT !.phase[t]="stopped"]
Advance(t) == WaitReady(t) \/ PollReady(t) \/ IOSuccess(t) \/ Release(t) \/ Commit(t) \/ FatalReturn(t) \/ RetryReady(t) \/ StopWaiting(t)
Done == /\ (\A t \in Tables:st.phase[t]="stopped") /\ UNCHANGED st
Next == (\E t \in Tables:Advance(t) \/ Acquire(t) \/ IOFailure(t) \/ IOFatal(t) \/ CommitError(t)) \/ Cancel \/ Done
Spec == Init /\ [][Next]_vars
 /\ (\A t \in Tables:WF_vars(Advance(t)) /\ SF_vars(Acquire(t)))
WaitingDoesNotHoldSlot == \A t \in Tables:
 st.phase[t] \in {"backpressure","workWait","pollWait","acquire","retry","commit","fatalReturn","stopped"} => st.holder # t
SlotMatchesActiveOperation == st.holder # "none" => st.phase[st.holder] \in {"io","release"}
FreeSlotAvailableToReadyTable == \A t \in Tables:
 (st.phase[t]="acquire" /\ st.holder="none") => ENABLED Acquire(t)
ShutdownReleasesSlot == (st.canceled /\ (\A t \in Tables:st.phase[t]="stopped")) => st.holder="none"
EveryTableFinishesWithFairAdmission == <>(\A t \in Tables:st.phase[t]="stopped")
FatalErrorStopsAllTables == st.groupError ~> (\A t \in Tables:st.phase[t]="stopped")
=============================================================================
