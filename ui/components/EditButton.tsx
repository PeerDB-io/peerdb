'use client';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { useRouter } from 'next/navigation';

const EditButton = ({ toLink }: { toLink: string }) => {
  const router = useRouter();
  return (
    <button
      className='IconButton'
      onClick={() => router.push(toLink)}
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
      <Icon name='edit' />
    </button>
  );
};

export default EditButton;
