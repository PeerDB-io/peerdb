'use client';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
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
    <Button
      className='IconButton'
      onClick={handleEdit}
      aria-label='sort up'
      variant='normal'
      style={{
        display: 'flex',
        alignItems: 'flex-start',
        columnGap: '0.3rem',
        width: '100%',
      }}
      disabled={disabled}
    >
      <Label>Edit Mirror</Label>
      {loading && <ProgressCircle variant='determinate_progress_circle' />}
    </Button>
  );
};

export default EditButton;
