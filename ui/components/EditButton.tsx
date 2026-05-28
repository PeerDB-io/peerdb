'use client';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { useRouter } from 'next/navigation';
import { useTransition } from 'react';

export default function EditButton({
  toLink,
  disabled,
}: {
  toLink: string;
  disabled: boolean;
}) {
  const [loading, startTransition] = useTransition();
  const router = useRouter();

  const handleEdit = () => {
    startTransition(() => {
      router.push(toLink);
    });
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
