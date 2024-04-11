'use client';
import { getMirrorState } from '@/app/mirrors/[mirrorId]/handlers';
import EditButton from '@/components/EditButton';
import { ResyncDialog } from '@/components/ResyncDialog';
import { FlowConnectionConfigs, FlowStatus } from '@/grpc_generated/flow';
import { MirrorStatusResponse } from '@/grpc_generated/route';
import { Select, SelectItem } from '@tremor/react';
import { useEffect, useState } from 'react';
import PauseOrResumeButton from './PauseOrResumeButton';

const MirrorActions = ({
  mirrorConfig,
  workflowId,
  editLink,
  canResync,
  isNotPaused,
}: {
  mirrorConfig: FlowConnectionConfigs;
  workflowId: string;
  editLink: string;
  canResync: boolean;
  isNotPaused: boolean;
}) => {
  const [mirrorStatus, setMirrorStatus] = useState<FlowStatus>();
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    getMirrorState(mirrorConfig.flowJobName).then(
      (res: MirrorStatusResponse) => {
        setMirrorStatus(res.currentFlowState);
      }
    );
    setMounted(true);
  }, [mirrorConfig.flowJobName]);

  if (mounted)
    return (
      <div>
        <Select
          placeholder='Actions'
          value='Actions'
          style={{ width: 'fit-content' }}
        >
          <SelectItem value='1' style={{ padding: 0 }}>
            {mirrorStatus && (
              <PauseOrResumeButton
                mirrorConfig={mirrorConfig}
                mirrorStatus={mirrorStatus}
              />
            )}
          </SelectItem>
          <SelectItem value='2' style={{ padding: 0 }}>
            <EditButton toLink={editLink} disabled={isNotPaused} />
          </SelectItem>

          <SelectItem value='3' style={{ padding: 0 }}>
            {canResync && (
              <ResyncDialog
                mirrorConfig={mirrorConfig}
                workflowId={workflowId}
              />
            )}
          </SelectItem>
        </Select>
      </div>
    );
  return <></>;
};

export default MirrorActions;
