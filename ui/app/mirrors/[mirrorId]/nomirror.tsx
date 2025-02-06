import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import Link from 'next/link';

export default function NoMirror() {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        rowGap: '1rem',
        justifyContent: 'center',
      }}
    >
      <Label variant='title2'>Oops! </Label>
      <Label as='label' style={{ fontSize: 18 }}>
        We were unable to fetch details of this mirror. Please confirm if this
        mirror exists.
      </Label>
      <Link href='/mirrors'>
        <Button style={{ padding: '1rem' }}>Back to mirrors page</Button>
      </Link>
    </div>
  );
}
