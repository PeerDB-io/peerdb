import { Header } from '@/lib/Header';
import LogsView from './table';

const MirrorLogs = async () => {
  return (
    <div
      style={{
        display: 'flex',
        width: '100%',
        padding: '2rem',
        flexDirection: 'column',
      }}
    >
      <Header variant='title2'>Logs</Header>
      <LogsView />
    </div>
  );
};

export default MirrorLogs;
