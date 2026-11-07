----------------------------- MODULE QueryConverter -----------------------------
EXTENDS Naturals
CONSTANTS Capacity, Records, CancelAware
VARIABLE st
vars == <<st>>
\* A single SyncQueryCDC call has started RecordsToTypedCDCStream.
Init == st=[read |-> 0, buffered |-> 0, child |-> "read", consumer |-> TRUE,
            canceled |-> FALSE, inputClosed |-> FALSE, returned |-> FALSE]
ReadRow == /\ st.child="read" /\ ~st.inputClosed /\ st.read<Records
 /\ st'=[st EXCEPT !.read=@+1, !.child="send"]
\* The actual send has no context arm. Closing input cannot unblock it.
SendRow == /\ st.child="send" /\ st.buffered<Capacity
 /\ st'=[st EXCEPT !.buffered=@+1, !.child="read"]
Consume == /\ st.consumer /\ st.buffered>0
 /\ st'=[st EXCEPT !.buffered=@-1]
StagingError == /\ st.consumer /\ ~st.canceled
 /\ st'=[st EXCEPT !.consumer=FALSE, !.canceled=TRUE]
PullStops == /\ ~st.inputClosed /\ (st.canceled \/ st.read=Records)
 /\ st'=[st EXCEPT !.inputClosed=TRUE]
ReadEOF == /\ st.child="read" /\ st.inputClosed
 /\ st'=[st EXCEPT !.child="stopped"]
\* pollGroup.Wait joins pull/staging, not the converter child.
ReturnGroup == /\ st.canceled /\ st.inputClosed /\ ~st.returned
 /\ st'=[st EXCEPT !.returned=TRUE]
\* Sensitivity control only: not present in either source snapshot.
CancelChild == /\ CancelAware /\ st.canceled /\ st.child # "stopped"
 /\ st'=[st EXCEPT !.child="stopped"]
Next == ReadRow \/ SendRow \/ Consume \/ StagingError \/ PullStops \/ ReadEOF \/ ReturnGroup \/ CancelChild
Spec == Init /\ [][Next]_vars
 /\ WF_vars(ReadRow) /\ WF_vars(SendRow) /\ WF_vars(Consume)
 /\ WF_vars(PullStops) /\ WF_vars(ReadEOF) /\ WF_vars(ReturnGroup) /\ WF_vars(CancelChild)
BufferBounded == st.buffered<=Capacity
TopLevelShutdownCompletes == st.canceled ~> st.returned
ConverterShutdownCompletes == st.canceled ~> (st.child="stopped")
=============================================================================
