'use client';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { ColumnWithTextField } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { Separator } from '@/lib/Separator';
import { TextField } from '@/lib/TextField';

export default function Login() {
  return (
    <>
      <Panel>
        <Label variant='title3' as='h1'>
          Sign in to PeerDB
        </Label>
        <ColumnWithTextField
          label={() => (
            <Label as='label' htmlFor='email'>
              Email
            </Label>
          )}
          action={() => (
            <TextField variant='simple' placeholder='Email' id='email' />
          )}
        />
        <ColumnWithTextField
          label={() => (
            <Label as='label' htmlFor='password'>
              Password
            </Label>
          )}
          action={() => (
            <TextField variant='simple' placeholder='Password' id='password' />
          )}
        />
        <Separator height='thin' variant='empty' />
        <Button variant='normalSolid' className='w-full'>
          Sign in
        </Button>
      </Panel>
      <Separator height='thin' variant='centered' />
      <Panel>
        <Button variant='normal' className='w-full'>
          Use SSO
        </Button>
        <Button variant='normal' className='w-full'>
          Forgotten password?
        </Button>
        <Button variant='normal' className='w-full'>
          Create new account
        </Button>
      </Panel>
    </>
  );
}
