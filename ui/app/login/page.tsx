'use client';
import Image from 'next/image';
import { useRouter, useSearchParams } from 'next/navigation';
import { useState } from 'react';

import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Layout } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';

export default function Login() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [pass, setPass] = useState('');
  const [show, setShow] = useState(false);
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
        if (res.error) {
          setError(res.error);
        } else router.push('/');
      });
  };
  return (
    <Layout>
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          width: 'full',
        }}
      >
        <Image
          style={{ marginBottom: '4rem' }}
          src='/images/peerdb-combinedMark.svg'
          alt='PeerDB'
          width={400}
          height={300}
        />
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            width: '20%',
            height: '4%',
          }}
        >
          <TextField
            variant='simple'
            placeholder='Password'
            type={show ? 'text' : 'password'}
            style={{
              width: '100%',
              height: '100%',
              borderRadius: '0.5rem',
              marginRight: '1rem',
            }}
            value={pass}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setPass(e.target.value)
            }
            onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => {
              if (e.key === 'Enter') {
                login();
              }
            }}
          />
          <Button onClick={() => setShow((curr) => !curr)}>
            <Icon name={show ? 'visibility_off' : 'visibility'} />
          </Button>
        </div>
        <Button
          style={{
            marginTop: '2rem',
            width: '6em',
            height: '2em',
            boxShadow: '0px 4px 4px rgba(0,0,0,0.15)',
          }}
          variant='normalSolid'
          onClick={login}
        >
          Log in
        </Button>
        {error && (
          <div
            style={{
              borderRadius: '0.2rem',
              padding: '0.5rem',
              color: '#d46363',
              marginTop: '1rem',
            }}
          >
            {error}
          </div>
        )}
      </div>
    </Layout>
  );
}
