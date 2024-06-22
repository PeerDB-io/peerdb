import prisma from '@/app/utils/prisma';
import { FlowStatus } from '@/grpc_generated/flow';
import { Header } from '@/lib/Header';
import { LayoutMain } from '@/lib/Layout';
import QrepGraph from './qrepGraph';
import QRepStatusTable, { QRepPartitionStatus } from './qrepStatusTable';

type QRepMirrorStatusProps = {
  params: { mirrorId: string };
};

function setFlowState(flowJobName: string, requestedFlowState: FlowStatus) {
  return fetch(`/api/mirrors/state_change`, {
    method: 'POST',
    body: JSON.stringify({
      flowJobName,
      requestedFlowState,
    }),
    cache: 'no-store',
  });
}

export default async function QRepMirrorStatus({
  params: { mirrorId },
}: QRepMirrorStatusProps) {
  const runs = await prisma.qrep_partitions.findMany({
    where: {
      flow_name: mirrorId,
      start_time: {
        not: null,
      },
      rows_in_partition: {
        not: 0,
      },
    },
    orderBy: {
      start_time: 'desc',
    },
  });

  const partitions: QRepPartitionStatus[] = runs.map((run) => ({
    partitionId: run.partition_uuid,
    startTime: run.start_time,
    endTime: run.end_time,
    pulledRows: run.rows_in_partition,
    syncedRows: Number(run.rows_synced),
  }));

  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          paddingRight: '2rem',
        }}
      >
        <Header variant='title2'>{mirrorId}</Header>
        <input
          type='button'
          value='Pause'
          onClick={() => setFlowState(mirrorId, FlowStatus.STATUS_PAUSED)}
        />
        <input
          type='button'
          value='Resume'
          onClick={() => setFlowState(mirrorId, FlowStatus.STATUS_RUNNING)}
        />
      </div>
      <QrepGraph
        syncs={partitions.map((partition) => ({
          partitionID: partition.partitionId,
          startTime: partition.startTime,
          endTime: partition.endTime,
          numRows: partition.pulledRows,
        }))}
      />
      <br></br>
      <QRepStatusTable flowJobName={mirrorId} partitions={partitions} />
    </LayoutMain>
  );
}
