'use client';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { useRouter } from 'next/navigation';
import { useState } from 'react';

export default function EditButton({
  toLink,
  disabled,
}: {
  toLink: string;
  disabled: boolean;
}) {
  const [loading, setLoading] = useState(false);
  const router = useRouter();

  const handleEdit = () => {
    setLoading(true);
    router.push(toLink);
  };
  return (
    <Button
      onClick={handleEdit}
      variant='normalBorderless'
      style={{
        display: 'flex',
        alignItems: 'flex-start',
        justifyContent: 'left',
        columnGap: '0.3rem',
        width: '100%',
      }}
      disabled={disabled}
    >
      <Label>Edit {disabled ? '(pause first)' : 'mirror'}</Label>
      {loading && <ProgressCircle variant='determinate_progress_circle' />}
    </Button>
  );
}
