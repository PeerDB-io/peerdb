import prisma from '@/app/utils/prisma';
import { Header } from '@/lib/Header';
import { LayoutMain } from '@/lib/Layout';
import QRepStatusTable, { QRepPartitionStatus } from './qrepStatusTable';

type QRepMirrorStatusProps = {
  params: { mirrorId: string };
};

export default async function QRepMirrorStatus({
  params: { mirrorId },
}: QRepMirrorStatusProps) {
  const runs = await prisma.qrep_partitions.findMany({
    where: {
      flow_name: mirrorId,
      start_time: {
        not: null,
      },
    },
    orderBy: {
      start_time: 'desc',
    },
  });

  const partitions = runs.map((run) => {
    let ret: QRepPartitionStatus = {
      partitionId: run.partition_uuid,
      runUuid: run.run_uuid,
      startTime: run.start_time,
      endTime: run.end_time,
      numRows: run.rows_in_partition,
      status: '',
    };
    return ret;
  });

  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <Header variant='title2'>{mirrorId}</Header>
      <QRepStatusTable flowJobName={mirrorId} partitions={partitions} />
    </LayoutMain>
  );
}
