'use client';
import { changeFlowState } from '@/app/mirrors/[mirrorId]/handlers';
import { notifyErr } from '@/app/utils/notify';
import { FlowStatus } from '@/grpc_generated/flow';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label/Label';
import { useRouter } from 'next/navigation';
import { useTransition } from 'react';

type PauseOrResumeButtonProps = {
  mirrorName: string;
  canDrain: boolean;
  mirrorStatus: FlowStatus;
};

export default function PauseOrResumeButton({
  mirrorStatus,
  mirrorName,
  canDrain,
}: PauseOrResumeButtonProps) {
  const [pending, start] = useTransition();
  const { refresh } = useRouter();

  const transition = (target: FlowStatus, drain = false) =>
    start(async () => {
      const res = await changeFlowState(mirrorName, target, false, drain);
      if (!res.ok) {
        notifyErr((await res.json()).message || res.statusText);
        return;
      }
      refresh();
    });

  const status = mirrorStatus.toString();
  if (status === FlowStatus[FlowStatus.STATUS_RUNNING]) {
    return (
      <>
        <Button
          variant='normalBorderless'
          style={{ width: '100%', justifyContent: 'left' }}
          disabled={pending}
          onClick={() => transition(FlowStatus.STATUS_PAUSED)}
        >
          <Label>Pause mirror</Label>
        </Button>
        {canDrain && (
          <Button
            variant='normalBorderless'
            style={{ width: '100%', justifyContent: 'left' }}
            disabled={pending}
            title='Wait for pending data to reach ClickHouse. Wait for Paused status before migrating.'
            onClick={() => transition(FlowStatus.STATUS_PAUSED, true)}
          >
            <Label>Pause and drain mirror</Label>
          </Button>
        )}
      </>
    );
  } else if (
    status === FlowStatus[FlowStatus.STATUS_PAUSED] ||
    status === FlowStatus[FlowStatus.STATUS_DRAINING]
  ) {
    return (
      <>
        {status === FlowStatus[FlowStatus.STATUS_DRAINING] && (
          <Button
            style={{ width: '100%', justifyContent: 'left' }}
            disabled={pending}
            onClick={() => transition(FlowStatus.STATUS_PAUSED)}
          >
            <Label>Pause without draining</Label>
          </Button>
        )}
        <Button
          style={{ width: '100%', justifyContent: 'left' }}
          disabled={pending}
          onClick={() => transition(FlowStatus.STATUS_RUNNING)}
        >
          <Label>Resume mirror</Label>
        </Button>
      </>
    );
  } else {
    return (
      <Button className='IconButton' aria-label='Pause (disabled)' disabled>
        <Label>Pause mirror</Label>
      </Button>
    );
  }
}
