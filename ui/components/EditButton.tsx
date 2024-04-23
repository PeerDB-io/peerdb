'use client';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { Tooltip } from '@/lib/Tooltip';
import { useRouter } from 'next/navigation';
import { useState } from 'react';

const EditButton = ({
  toLink,
  disabled,
}: {
  toLink: string;
  disabled: boolean;
}) => {
  const [loading, setLoading] = useState(false);
  const router = useRouter();

  const handleEdit = () => {
    setLoading(true);
    router.push(toLink);
  };
  return (
    <Tooltip
      style={{
        display: disabled ? 'flex' : 'none',
        backgroundColor: 'white',
        color: 'black',
        width: '100%',
      }}
      content='Pause the mirror to enable editing'
    >
      <Button
        className='IconButton'
        onClick={handleEdit}
        aria-label='sort up'
        variant='normalBorderless'
        style={{
          display: 'flex',
          alignItems: 'flex-start',
          columnGap: '0.3rem',
          width: '100%',
        }}
        disabled={disabled}
      >
        <Label>Edit mirror</Label>
        {loading && <ProgressCircle variant='determinate_progress_circle' />}
      </Button>
    </Tooltip>
  );
};

export default EditButton;
