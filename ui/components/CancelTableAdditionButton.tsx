'use client';
import { getMirrorState } from '@/app/mirrors/[mirrorId]/handlers';
import { notifyErr } from '@/app/utils/notify';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label/Label';
import { useRouter } from 'next/navigation';
import { useTransition } from 'react';

export default function CancelTableAdditionButton({
  mirrorName,
}: {
  mirrorName: string;
}) {
  const [pending, start] = useTransition();
  const { refresh } = useRouter();

  return (
    <Button
      variant='normalBorderless'
      style={{ width: '100%', justifyContent: 'left', textAlign: 'left' }}
      disabled={pending}
      onClick={() =>
        start(async () => {
          const state = await getMirrorState(mirrorName);
          const res = await fetch('/api/v1/flows/cdc/cancel_table_addition', {
            method: 'POST',
            cache: 'no-store',
            body: JSON.stringify({
              flowJobName: mirrorName,
              currentlyReplicatingTables:
                state.cdcStatus?.config?.tableMappings ?? [],
              idempotencyKey: crypto.randomUUID(),
              assumeTableRemovalWillNotHappen: false,
            }),
          });
          if (!res.ok) {
            notifyErr((await res.json()).message || res.statusText);
            return;
          }
          notifyErr('Cancelled in-flight table addition', true);
          refresh();
        })
      }
    >
      <Label>Cancel adding tables</Label>
    </Button>
  );
}
