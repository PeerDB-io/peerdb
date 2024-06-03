import { Header } from '@/lib/Header';
import 'react-toastify/dist/ReactToastify.css';
import prisma from '../utils/prisma';
import LogsView from './table';
export const revalidate = 10;

export default async function MirrorLogs() {
  const mirrorNames = await prisma.flows.findMany({
    select: {
      name: true,
    },
    distinct: ['name'],
  });

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
}
