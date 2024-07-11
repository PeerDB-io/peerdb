import { Header } from '@/lib/Header';
import LogsView from './table';

export default async function MirrorLogs() {
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
}
