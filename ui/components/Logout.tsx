'use client';
import { Button } from '@/lib/Button';

export default function Logout() {
  return (
    <Button
      onClick={() =>
        fetch('/api/logout', { method: 'POST' }).then((res) =>
          location.assign('/login')
        )
      }
    >
      Logout
    </Button>
  );
}
