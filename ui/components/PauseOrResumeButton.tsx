'use client';
import { changeFlowState } from '@/app/mirrors/[mirrorId]/handlers';
import { FlowConnectionConfigs, FlowStatus } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label/Label';

function PauseOrResumeButton({
  mirrorConfig,
  mirrorStatus,
}: {
  mirrorConfig: FlowConnectionConfigs;
  mirrorStatus: FlowStatus;
}) {
  if (mirrorStatus.toString() === FlowStatus[FlowStatus.STATUS_RUNNING]) {
    return (
      <Button
        className='IconButton'
        aria-label='Pause'
        variant='normalBorderless'
        onClick={() => changeFlowState(mirrorConfig, FlowStatus.STATUS_PAUSED)}
      >
        <Label>Pause mirror</Label>
      </Button>
    );
  } else if (mirrorStatus.toString() === FlowStatus[FlowStatus.STATUS_PAUSED]) {
    return (
      <Button
        className='IconButton'
        aria-label='Play'
        onClick={() => changeFlowState(mirrorConfig, FlowStatus.STATUS_RUNNING)}
      >
        <Label>Resume mirror</Label>
      </Button>
    );
  } else {
    return (
      <Button
        className='IconButton'
        aria-label='Pause (disabled)'
        disabled={true}
      >
        <Label>Pause mirror</Label>
      </Button>
    );
  }
}

export default PauseOrResumeButton;
