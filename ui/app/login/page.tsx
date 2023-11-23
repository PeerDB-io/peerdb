'use client';
import Image from 'next/image';
import { useRouter, useSearchParams } from 'next/navigation';
import { useState } from 'react';

import { Button } from '@/lib/Button';
import { Layout, LayoutMain } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';

export default function Login() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [pass, setPass] = useState('');
  const [error, setError] = useState(() =>
    searchParams.has('reject') ? 'Authentication failed, please login' : ''
  );
  const login = () => {
    fetch('/api/login', {
      method: 'POST',
      body: JSON.stringify({ password: pass }),
    })
      .then((res) => res.json())
      .then((res) => {
        if (res.error) setError(res.error);
        else router.push('/');
      });
  };
  return (
    <Layout>
      <LayoutMain alignSelf='center' justifySelf='center' width='xxLarge'>
        <Image src='/images/peerdb-combinedMark.svg' alt='PeerDB' width={512} />
        {error && (
          <div
            style={{
              borderRadius: '8px',
              fontWeight: 'bold',
              color: '#600',
              backgroundColor: '#c66',
            }}
          >
            {error}
          </div>
        )}
        <TextField
          variant='simple'
          placeholder='Password'
          value={pass}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
            setPass(e.target.value)
          }
          onKeyPress={(e: React.KeyboardEvent<HTMLInputElement>) => {
            if (e.key === 'Enter') {
              login();
            }
          }}
        />
        <Button onClick={login}>Login</Button>
      </LayoutMain>
    </Layout>
  );
}
