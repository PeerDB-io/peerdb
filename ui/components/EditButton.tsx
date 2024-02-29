'use client';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { useRouter } from 'next/navigation';
import { useState } from 'react';

const EditButton = ({ toLink }: { toLink: string }) => {
  const [loading, setLoading] = useState(false);
  const router = useRouter();

  const handleEdit = () => {
    setLoading(true);
    router.push(toLink);
  };
  return (
    <button
      className='IconButton'
      onClick={handleEdit}
      aria-label='sort up'
      style={{
        display: 'flex',
        marginLeft: '1rem',
        alignItems: 'center',
        backgroundColor: 'whitesmoke',
        border: '1px solid rgba(0,0,0,0.1)',
        borderRadius: '0.5rem',
      }}
    >
      <Label>Edit Mirror</Label>
      {loading ? (
        <ProgressCircle variant='determinate_progress_circle' />
      ) : (
        <Icon name='edit' />
      )}
    </button>
  );
};

export default EditButton;
