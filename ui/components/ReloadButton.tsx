'use client';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';

export default function ReloadButton() {
  return (
    <Button
      variant='normalSolid'
      style={{ fontSize: 14 }}
      onClick={() => window.location.reload()}
    >
      Refresh <Icon name='refresh' />
    </Button>
  );
}
