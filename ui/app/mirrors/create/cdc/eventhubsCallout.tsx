import { useTheme } from '@/lib/AppTheme';

const EventhubsCallout = () => {
  const theme = useTheme();
  const isDarkMode = theme.theme === 'dark';
  return (
    <div
      style={{
        marginTop: '1rem',
        backgroundColor: isDarkMode ? '#7f1d1d33' : '#fef2f2',
        borderLeft: '4px solid' + (isDarkMode ? '#f87171' : '#f87171'),
        padding: '1rem',
        borderRadius: '0.375rem',
        color: isDarkMode ? '#fca5a5' : '#991b1b',
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
    </div>
  );
};

export default EventhubsCallout;
