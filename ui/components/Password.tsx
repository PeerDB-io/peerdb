'use client';
import { Button } from '@/lib/Button';

export default function Password() {
  return (
    <>
      <input id='password' type='password' />
      <Button
        onClick={() => {
          fetch('/api/login', {
            method: 'POST',
            body: JSON.stringify({
              password: (document.getElementById('password') as any).value,
            }),
          });
        }}
      >
        Login
      </Button>
    </>
  );
}
