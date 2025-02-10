'use client';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useState } from 'react';

export default function MirrorLink({ flowName }: { flowName: string }) {
  const router = useRouter();
  const [isLoading, setIsLoading] = useState(false);
  return (
    <div>
      {isLoading ? (
        <ProgressCircle variant='determinate_progress_circle' />
      ) : (
        <Link
          href={`/mirrors/${flowName}`}
          onClick={() => {
            setIsLoading(true);
            router.push(`/mirrors/${flowName}`);
          }}
        >
          <Label>
            <div className='cursor-pointer underline'>{flowName}</div>
          </Label>
        </Link>
      )}
    </div>
  );
}
