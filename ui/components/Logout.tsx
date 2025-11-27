'use client';
import { Button } from '@/lib/Button';
import { useSession } from 'next-auth/react';
import { useRouter } from 'next/navigation';

export default function Logout() {
  const { data: session } = useSession();
  const router = useRouter();
  if (session) {
    return (
      <Button variant='normal' onClick={() => router.push('/api/auth/signout')}>
        Log out
      </Button>
    );
  }
}
