import { Header } from '@/lib/Header';
import prisma from '../utils/prisma';
import LogsView from './table';

const fetchMirrors = async () => {
  const mirrorNames = await prisma.flows.findMany({
    select: {
      name: true,
    },
    distinct: ['name'],
  });

  return mirrorNames.map((mirror) => mirror.name);
};

const MirrorLogs = async () => {
  const mirrorNames = await fetchMirrors();
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
      <LogsView mirrors={mirrorNames} />
    </div>
  );
};

export default MirrorLogs;
