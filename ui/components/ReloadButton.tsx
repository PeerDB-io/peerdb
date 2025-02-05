'use client';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';

export default function ReloadButton() {
  return (
    <Button
      style={{ backgroundColor: '#30A46C', color: 'white', fontSize: 14 }}
      onClick={() => window.location.reload()}
    >
      Refresh <Icon name='refresh' />
    </Button>
  );
}
