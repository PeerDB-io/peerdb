'use client';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import Link from 'next/link';
import { useState } from 'react';

const MirrorLink = ({ flowName }: { flowName: string }) => {
  const [isLoading, setIsLoading] = useState(false);
  return (
    <div onClick={() => setIsLoading(true)}>
      {isLoading ? (
        <ProgressCircle variant='determinate_progress_circle' />
      ) : (
        <Link href={`/mirrors/edit/${flowName}`}>
          <Label>
            <div className='cursor-pointer underline'>{flowName}</div>
          </Label>
        </Link>
      )}
    </div>
  );
};
export default MirrorLink;
