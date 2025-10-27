const EventhubsCallout = () => {
  return (
    <div
      style={{
        marginTop: '1rem',
        backgroundColor: '#fef2f2',
        borderLeft: '4px solid #f87171',
        padding: '1rem',
        borderRadius: '0.375rem',
        color: '#991b1b',
      }}
    >
      <div style={{ marginBottom: '0.5rem', fontWeight: 'bold' }}>
        Note on Eventhubs targets
      </div>
      <div style={{ fontSize: 14 }}>
        Eventhubs targets are of the form{' '}
        <b>namespace.eventhub_name.partition_column</b>.<br />
        Namespaces are specified in the Eventhub peer. PeerDB will create the
        eventhub if needed with the name you specify in the provided namespace
        for each source table.
        <br />
        Messages are sent to partitions based on the values of the partition
        column.
      </div>
      <div style={{ marginTop: '1rem' }}>
        Mirror can only be edited while paused.
      </div>
    </div>
  );
};

export default EventhubsCallout;
