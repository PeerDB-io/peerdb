'use client';
import Image from 'next/image';
import { useState } from 'react';

import { Button } from '@/lib/Button';
import { Layout, LayoutMain } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';

export default function Password() {
  const [pass, setPass] = useState('');
  const [error, setError] = useState('');
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
        />
        <Button
          onClick={() => {
            fetch('/api/login', {
              method: 'POST',
              body: JSON.stringify({ password: pass }),
            })
              .then((res) => res.json())
              .then((res) => {
                setError(res.error);
              });
          }}
        >
          Login
        </Button>
      </LayoutMain>
    </Layout>
  );
}
