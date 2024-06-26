'use client';
import { changeFlowState } from '@/app/mirrors/[mirrorId]/handlers';
import { FlowStatus } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label/Label';

type PauseOrResumeButtonProps = {
  mirrorName: string;
  mirrorStatus: FlowStatus;
};

function PauseOrResumeButton({
  mirrorStatus,
  mirrorName,
}: PauseOrResumeButtonProps) {
  if (mirrorStatus.toString() === FlowStatus[FlowStatus.STATUS_RUNNING]) {
    return (
      <Button
        variant='normalBorderless'
        style={{ width: '100%', justifyContent: 'left' }}
        onClick={() => changeFlowState(mirrorName, FlowStatus.STATUS_PAUSED)}
      >
        <Label>Pause mirror</Label>
      </Button>
    );
  } else if (mirrorStatus.toString() === FlowStatus[FlowStatus.STATUS_PAUSED]) {
    return (
      <Button
        style={{ width: '100%', justifyContent: 'left' }}
        onClick={() => changeFlowState(mirrorName, FlowStatus.STATUS_RUNNING)}
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
