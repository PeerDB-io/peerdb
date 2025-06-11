import { Button } from '@/lib/Button/Button';
import { Icon } from '@/lib/Icon/Icon';
import { Label } from '@/lib/Label/Label';
import Link from 'next/link';

export default function NewButton({
  targetPage,
  buttonText,
}: {
  targetPage: string;
  buttonText: string;
}) {
  return (
    <Button as={Link} href={targetPage} variant='normalSolid'>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          whiteSpace: 'nowrap',
        }}
      >
        <Icon name='add' />
        <Label>{buttonText}</Label>
      </div>
    </Button>
  );
}
