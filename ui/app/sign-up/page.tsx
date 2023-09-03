'use client';

import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { ColumnWithTextField } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { Separator } from '@/lib/Separator';
import { TextField } from '@/lib/TextField';
import styled from 'styled-components';

const NameInputWrapper = styled.div`
  display: flex;
  flex-flow: row nowrap;
  column-gap: ${({ theme }) => theme.spacing.medium};
`;

export default function SignUp() {
  return (
    <>
      <Panel>
        <Label variant='title3' as='h1'>
          Sign up to PeerDB
        </Label>
        <NameInputWrapper>
          <ColumnWithTextField
            label={() => (
              <Label as='label' htmlFor='name'>
                Name
              </Label>
            )}
            action={() => (
              <TextField variant='simple' placeholder='Name' id='name' />
            )}
          />
          <ColumnWithTextField
            label={() => (
              <Label as='label' htmlFor='surname'>
                Surname
              </Label>
            )}
            action={() => (
              <TextField variant='simple' placeholder='Surname' id='surname' />
            )}
          />
        </NameInputWrapper>

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
            <Label as='label' htmlFor='company'>
              Company
            </Label>
          )}
          action={() => (
            <TextField variant='simple' placeholder='Company' id='company' />
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
        <ColumnWithTextField
          label={() => (
            <Label as='label' htmlFor='password-confirmation'>
              Confirm your password
            </Label>
          )}
          action={() => (
            <TextField
              variant='simple'
              placeholder='Confirm password'
              id='password-confirmation'
            />
          )}
        />
        <Separator height='thin' variant='empty' />
        <Button variant='normalSolid' className='w-full'>
          Sign up
        </Button>
      </Panel>
      <Panel>
        <Separator height='thin' variant='centered' />
        <Label variant='footnote'>
          By clicking Sign Up, you agree to our Terms. Learn how we collect, use
          and share your data in our Privacy Policy and how we use cookies and
          similar technology in our Cookies Policy.
        </Label>
      </Panel>
    </>
  );
}
