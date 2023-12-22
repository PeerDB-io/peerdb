'use client';

import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
export const ErrorModal = ({ flowName }: { flowName: string }) => {
  const router = useRouter();
  return (
    <Link href={`/mirrors/errors/${flowName}`}>
      <Button
        style={{
          backgroundColor: 'rgba(240, 128, 128, 0.5)',
          height: '2rem',
          boxShadow: '0 2px 4px rgba(0, 0, 0, 0.2)',
        }}
      >
        <Label as='label' style={{ fontSize: 13, color: 'darkred' }}>
          Show errors
        </Label>
      </Button>
    </Link>
  );
};

export const MirrorError = ({ flowName }: { flowName: string }) => {
  const [flowStatus, setFlowStatus] = useState<string>();
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      try {
        const response = await fetch(`/api/mirrors/alerts`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ flowName }),
        });

        if (!response.ok) {
          throw new Error('Network response was not ok');
        }

        const flowStatus = await response.json();
        setFlowStatus(flowStatus);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [flowName]);

  if (isLoading) {
    return (
      <div>
        <ProgressCircle variant='intermediate_progress_circle' />
      </div>
    );
  }

  if (error) {
    return (
      <div>
        <Icon name='error' />
      </div>
    );
  }

  if (flowStatus == 'healthy') {
    return <Icon name='check_circle' fill={true} />;
  }

  return <ErrorModal flowName={flowName} />;
};
