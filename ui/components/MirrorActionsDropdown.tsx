'use client';
import { getMirrorState } from '@/app/mirrors/[mirrorId]/handlers';
import EditButton from '@/components/EditButton';
import { ResyncDialog } from '@/components/ResyncDialog';
import { FlowStatus } from '@/grpc_generated/flow';
import { MirrorStatusResponse } from '@/grpc_generated/route';
import { Select } from '@tremor/react';
import { useEffect, useState } from 'react';
import PauseOrResumeButton from './PauseOrResumeButton';

type MirrorActionsProps = {
  mirrorName: string;
  editLink: string;
  canResync: boolean;
  isNotPaused: boolean;
};

const MirrorActions = ({
  mirrorName,
  editLink,
  canResync,
  isNotPaused,
}: MirrorActionsProps) => {
  const [mirrorStatus, setMirrorStatus] = useState<FlowStatus>();
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    getMirrorState(mirrorName).then((res: MirrorStatusResponse) => {
      setMirrorStatus(res.currentFlowState);
    });
    setMounted(true);
  }, [mirrorName]);

  if (mounted)
    return (
      <div>
        <Select placeholder='Actions' value='Actions'>
          {mirrorStatus && (
            <PauseOrResumeButton
              mirrorName={mirrorName}
              mirrorStatus={mirrorStatus}
            />
          )}
          <EditButton toLink={editLink} disabled={isNotPaused} />
          {canResync && <ResyncDialog mirrorName={mirrorName} />}
        </Select>
      </div>
    );
  return <></>;
};

export default MirrorActions;
