'use client';
import { FormatStatus } from '@/app/utils/flowstatus';
import MirrorActions from '@/components/MirrorActionsDropdown';
import { FlowStatus } from '@/grpc_generated/flow';
import { DBType, dBTypeFromJSON } from '@/grpc_generated/peers';
import { MirrorStatusResponse } from '@/grpc_generated/route';
import { Badge } from '@/lib/Badge';
import { Header } from '@/lib/Header';
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { useCallback, useEffect, useState } from 'react';
import { CDCMirror } from './cdc';
import { getMirrorState } from './handlers';
import NoMirror from './nomirror';
import QrepGraph from './qrepGraph';
import QRepStatusButtons from './qrepStatusButtons';
import QRepStatusTable from './qrepStatusTable';
import SyncStatus from './syncStatus';

type EditMirrorProps = {
  params: { mirrorId: string };
};

export default function ViewMirror({ params: { mirrorId } }: EditMirrorProps) {
  const [mirrorState, setMirrorState] = useState<MirrorStatusResponse>();
  const [errorMessage, setErrorMessage] = useState('');
  const [mounted, setMounted] = useState(false);

  const fetchState = useCallback(async () => {
    setMounted(true);
    try {
      const res = await getMirrorState(mirrorId);
      setMirrorState(res);
    } catch (ex: any) {
      setErrorMessage(ex.message);
    }
  }, [mirrorId]);
  useEffect(() => {
    fetchState();
  }, [fetchState]);

  if (!mounted) {
    return <></>;
  }

  if (errorMessage) {
    return <NoMirror />;
  }

  let syncStatusChild = null;
  let actionsDropdown = null;

  if (mirrorState?.cdcStatus) {
    syncStatusChild = <SyncStatus flowJobName={mirrorId} />;

    const dbType = dBTypeFromJSON(mirrorState.cdcStatus.destinationType);

    const isNotPaused =
      mirrorState.currentFlowState.toString() !==
      FlowStatus[FlowStatus.STATUS_PAUSED];
    const canResync =
      mirrorState.currentFlowState.toString() !==
        FlowStatus[FlowStatus.STATUS_SETUP] &&
      (dbType.valueOf() === DBType.BIGQUERY.valueOf() ||
        dbType.valueOf() === DBType.SNOWFLAKE.valueOf() ||
        dbType.valueOf() === DBType.POSTGRES.valueOf() ||
        dbType.valueOf() === DBType.CLICKHOUSE.valueOf());

    actionsDropdown = (
      <MirrorActions
        mirrorName={mirrorId}
        editLink={`/mirrors/${mirrorId}/edit`}
        canResync={canResync}
        isNotPaused={isNotPaused}
      />
    );

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
          {actionsDropdown}
        </div>
        <CDCMirror syncStatusChild={syncStatusChild} status={mirrorState} />
      </LayoutMain>
    );
  } else if (mirrorState?.qrepStatus) {
    return (
      <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            paddingRight: '2rem',
            marginBottom: '1rem',
          }}
        >
          <Header variant='title2'>{mirrorId}</Header>
          <QRepStatusButtons mirrorId={mirrorId} />
        </div>
        <Label>
          Status: <Badge>{FormatStatus(mirrorState.currentFlowState)}</Badge>
        </Label>
        <QrepGraph syncs={mirrorState.qrepStatus.partitions} />
        <br></br>
        <QRepStatusTable
          flowJobName={mirrorId}
          partitions={mirrorState.qrepStatus.partitions}
        />
      </LayoutMain>
    );
  }
}
