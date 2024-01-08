'use client';
import { Button } from '@/lib/Button';
import { useRouter } from 'next/navigation';

export default function Logout() {
  const router = useRouter();
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
