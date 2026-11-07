----------------------------- MODULE Coordinator -----------------------------
EXTENDS Integers, FiniteSets
CONSTANTS SubscribeFirst, CheckClose, Updates, LastValue
VARIABLE st
vars == <<st>>
Init == st=[value |-> 0, highest |-> 0, channel |-> 0, closed |-> {}, producer |-> "store", waiter |-> IF CheckClose THEN "done" ELSE IF SubscribeFirst THEN "subscribe" ELSE "check", captured |-> 0, old |-> 0, completed |-> 0, closedCalled |-> FALSE, canceled |-> FALSE]
Store == /\ st.producer="store" /\ st'=[st EXCEPT !.value=IF st.completed=Updates-1 THEN LastValue ELSE 1, !.highest=1, !.producer="swap"]
Swap == /\ st.producer="swap" /\ st'=[st EXCEPT !.old=st.channel, !.channel=st.completed+1, !.producer="close"]
CloseOld == /\ st.producer="close" /\ st'=[st EXCEPT !.closed=IF st.old=-1 THEN @ ELSE @ \cup {st.old}, !.completed=@+1, !.producer=IF st.completed+1=Updates THEN "done" ELSE "store"]
Check == /\ st.waiter="check" /\ st'=[st EXCEPT !.waiter=IF st.value=1 THEN "done" ELSE IF SubscribeFirst THEN "sleep" ELSE "subscribe"]
Subscribe == /\ st.waiter="subscribe" /\ st'=[st EXCEPT !.captured=st.channel, !.waiter=IF SubscribeFirst THEN "check" ELSE "sleep"]
Wake == /\ st.waiter="sleep" /\ st.captured \in st.closed /\ st'=[st EXCEPT !.waiter=IF SubscribeFirst THEN "subscribe" ELSE "check"]
Cancel == /\ ~st.canceled /\ st'=[st EXCEPT !.canceled=TRUE]
CancelWait == /\ st.canceled /\ st.waiter="sleep" /\ st'=[st EXCEPT !.waiter="done"]
CloseNotifications == /\ CheckClose /\ ~st.closedCalled /\ st'=[st EXCEPT !.channel=-1, !.closed=@ \cup {st.channel}, !.closedCalled=TRUE]
Done == /\ st.producer="done" /\ st.waiter="done" /\ UNCHANGED st
Next == Store \/ Swap \/ CloseOld \/ Check \/ Subscribe \/ Wake \/ Cancel \/ CancelWait \/ CloseNotifications \/ Done
Spec == Init /\ [][Next]_vars /\ WF_vars(Store) /\ WF_vars(Swap) /\ WF_vars(CloseOld) /\ WF_vars(Check) /\ WF_vars(Subscribe) /\ WF_vars(Wake) /\ WF_vars(CancelWait)
NoStrandedWaiter == ~(st.producer="done" /\ st.waiter="sleep" /\ st.captured \notin st.closed /\ st.value=1 /\ ~st.canceled)
NoRegression == st.value=st.highest
EventuallyResumes == <>(st.waiter="done")
CancellationReleasesWaiter == st.canceled ~> (st.waiter="done")
CloseIsPermanent == st.closedCalled => st.channel=-1
=============================================================================
