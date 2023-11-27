'use client';
import { Button } from '@/lib/Button';

export default function Logout() {
  return (
    <Button
      style={{ backgroundColor: 'white', border: '1px solid rgba(0,0,0,0.15)' }}
      onClick={() =>
        fetch('/api/logout', { method: 'POST' }).then((res) =>
          location.assign('/login')
        )
      }
    >
      Log out
    </Button>
  );
}
