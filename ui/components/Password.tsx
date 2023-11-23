'use client';
import { Button } from '@/lib/Button';
import { TextField } from '@/lib/TextField';
import { useState } from 'react';

export default function Password() {
  const [pass, setPass] = useState("");
  const [error, setError] = useState("");
  return (
    <>
      {error && <div style={{
        borderRadius:'8px',
        fontWeight:'bold',
        color:'#600',
        backgroundColor:'#c66'
        }}>{error}</div>}
      Password: <TextField variant='simple' value={pass} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setPass(e.target.value)} />
      <Button
        onClick={() => {
          fetch('/api/login', {
            method: 'POST',
            body: JSON.stringify({password: pass}),
          }).then((res: any) => {
            setError(res.error);
          });
        }}
      >
        Login
      </Button>
    </>
  );
}
