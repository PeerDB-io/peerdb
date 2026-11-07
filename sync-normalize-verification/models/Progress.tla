---------------------------- MODULE Progress ----------------------------
EXTENDS Naturals
CONSTANTS Batches, Limit
VARIABLE st
vars == <<st>>
Init == \E s \in 0..Batches: \E n \in 0..s:
 st=[s |-> s,n |-> n,g |-> 0,rc |-> 0,sc |-> 0,r |-> "check",w |-> "check",target |-> 0,cancel |-> FALSE]
RecordSync(b) == /\ ~st.cancel /\ b \in 0..Batches
 /\ st'=[st EXCEPT !.s=IF b>st.s THEN b ELSE @, !.g=IF b>st.s THEN @+1 ELSE @]
RecordNorm(b) == /\ ~st.cancel /\ b \in 0..st.s
 /\ st'=[st EXCEPT !.n=IF b>st.n THEN b ELSE @, !.g=IF b>st.n THEN @+1 ELSE @]
FinalSync == RecordSync(Batches)
FinalNorm == RecordNorm(Batches)
CheckRequest == /\ st.r="check"
 /\ st'=IF st.cancel THEN [st EXCEPT !.r="canceled"]
 ELSE IF st.s>st.n THEN [st EXCEPT !.r="ready", !.target=st.s]
 ELSE [st EXCEPT !.r="armed", !.rc=st.g]
CheckCapacity == /\ st.w="check"
 /\ st'=IF st.cancel THEN [st EXCEPT !.w="canceled"]
 ELSE IF st.s-st.n<Limit THEN [st EXCEPT !.w="ready"]
 ELSE [st EXCEPT !.w="armed", !.sc=st.g]
SelectRequest == /\ st.r="armed" /\ st'=[st EXCEPT !.r="sleep"]
SelectCapacity == /\ st.w="armed" /\ st'=[st EXCEPT !.w="sleep"]
WakeRequest == /\ st.r="sleep" /\ (st.cancel \/ st.rc<st.g)
 /\ st'=[st EXCEPT !.r="check"]
WakeCapacity == /\ st.w="sleep" /\ (st.cancel \/ st.sc<st.g)
 /\ st'=[st EXCEPT !.w="check"]
Cancel == /\ ~st.cancel /\ st'=[st EXCEPT !.cancel=TRUE]
Idle == UNCHANGED st
Next == (\E b \in 0..Batches: RecordSync(b) \/ RecordNorm(b)) \/ CheckRequest \/ CheckCapacity
 \/ SelectRequest \/ SelectCapacity \/ WakeRequest \/ WakeCapacity \/ Cancel \/ Idle
Spec == Init /\ [][Next]_vars
 /\ WF_vars(FinalSync) /\ WF_vars(FinalNorm)
 /\ WF_vars(CheckRequest) /\ WF_vars(CheckCapacity) /\ WF_vars(SelectRequest) /\ WF_vars(SelectCapacity)
 /\ WF_vars(WakeRequest) /\ WF_vars(WakeCapacity)
Ordered == st.n<=st.s
NoLostRequest == (st.r \in {"armed","sleep"} /\ st.s>st.n) => st.rc<st.g
NoLostCompletion == (st.w \in {"armed","sleep"} /\ st.s-st.n<Limit) => st.sc<st.g
Monotone == [][st'.s>=st.s /\ st'.n>=st.n]_vars
DuplicatesDoNotReplaceChannel == [][((\E b \in 0..st.s: RecordSync(b)) \/ (\E b \in 0..st.n: RecordNorm(b))) => st'.g=st.g]_vars
ReadyRequestHasWork == [][(CheckRequest /\ st'.r="ready") => (st'.target=st.s /\ st.s>st.n /\ ~st.cancel)]_vars
CapacityBoundary == [][(CheckCapacity /\ ~st.cancel) => ((st'.w="ready") <=> st.s-st.n<Limit)]_vars
CancellationObserved == st.cancel ~> (st.r \in {"ready","canceled"} /\ st.w \in {"ready","canceled"})
CatchupEventuallyReturns == <>(st.w \in {"ready","canceled"})
=============================================================================
