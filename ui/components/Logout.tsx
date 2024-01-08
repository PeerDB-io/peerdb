'use client';
import { Button } from '@/lib/Button';
import { useRouter } from 'next/navigation';
import { useSession } from 'next-auth/react';

export default function Logout() {
  const { data: session } = useSession();
  const router = useRouter();
  if (session) {
    return (
      <Button
        style={{ backgroundColor: 'white', border: '1px solid rgba(0,0,0,0.15)' }}
        onClick={() => router.push('/api/auth/signout')
        }
      >
        Log out
      </Button>
    );
  }
}
