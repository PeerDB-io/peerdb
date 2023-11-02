'use client';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';

const ReloadButton = () => {
  return (
    <Button
      style={{ backgroundColor: '#30A46C', color: 'white' }}
      onClick={() => window.location.reload()}
    >
      Refresh <Icon name='refresh' />
    </Button>
  );
};

export default ReloadButton;
