'use client';

import { Action } from '@/lib/Action';
import { Avatar } from '@/lib/Avatar';
import { Button } from '@/lib/Button';
import { Header } from '@/lib/Header';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain, RowWithSelect, RowWithTextField } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { Select } from '@/lib/Select';
import { TextField } from '@/lib/TextField';

export default function UserSettings() {
  return (
    <LayoutMain alignSelf='flex-start' justifySelf='center' topPadding>
      <Panel>
        <Avatar size='xLarge' variant='text' text='NS' />
        <Label as='h1' variant='title3'>
          Name Surname
        </Label>
        <Label variant='footnote'>Email address</Label>
        <Label variant='footnote'>ID: namesurname</Label>
        <Action icon={() => <Icon name='add_a_photo' />}>Edit photo</Action>
      </Panel>
      <Panel>
        <Label
          variant='subheadline'
          as='h2'
          colorSet='base'
          colorName='lowContrast'
        >
          Profile information
        </Label>
        <RowWithTextField
          label={() => (
            <Label as='label' htmlFor='name'>
              Name
            </Label>
          )}
          action={() => (
            <TextField id='name' placeholder='Name' variant='simple' />
          )}
        />
        <RowWithTextField
          label={() => (
            <Label as='label' htmlFor='company'>
              Company
            </Label>
          )}
          action={() => (
            <TextField id='company' placeholder='Company' variant='simple' />
          )}
        />
        <RowWithTextField
          label={() => (
            <Label as='label' htmlFor='company-website'>
              Company website
            </Label>
          )}
          action={() => (
            <TextField
              id='company-website'
              placeholder='Company Website'
              variant='simple'
            />
          )}
        />
        <RowWithTextField
          label={() => (
            <Label as='label' htmlFor='email'>
              Email
            </Label>
          )}
          action={() => (
            <TextField
              id='email'
              placeholder='Email'
              variant='simple'
              disabled
            />
          )}
        />
        <Action icon={() => <Icon name='mail' />}>Change email</Action>
      </Panel>
      <Panel>
        <Label
          variant='subheadline'
          as='h2'
          colorSet='base'
          colorName='lowContrast'
        >
          Settings
        </Label>
        <RowWithSelect
          label={() => (
            <Label as='label' htmlFor='appearance'>
              Appearance
            </Label>
          )}
          action={() => <Select id='appearance' placeholder='Appearance' />}
        />
        <RowWithSelect
          label={() => (
            <Label as='label' htmlFor='company'>
              Time zone
            </Label>
          )}
          action={() => (
            <Select id='appearance' placeholder='Stockholm (GMT+2)' />
          )}
        />
      </Panel>
      <Panel>
        <Label
          variant='subheadline'
          as='h2'
          colorSet='base'
          colorName='lowContrast'
        >
          Security
        </Label>
        <RowWithTextField
          label={() => (
            <Label as='label' htmlFor='password'>
              Password
            </Label>
          )}
          action={() => (
            <TextField
              id='password'
              placeholder='Password'
              variant='simple'
              disabled
            />
          )}
        />
        <Action icon={() => <Icon name='key' />}>Change password</Action>
      </Panel>
      <Panel>
        <Label
          variant='subheadline'
          as='h2'
          colorSet='base'
          colorName='lowContrast'
        >
          Billing information
        </Label>
        <RowWithTextField
          label={() => (
            <Label as='label' htmlFor='address'>
              Address
            </Label>
          )}
          action={() => (
            <TextField id='address' placeholder='Address' variant='simple' />
          )}
        />
        <RowWithTextField
          label={() => (
            <Label as='label' htmlFor='co'>
              C/O
            </Label>
          )}
          action={() => (
            <TextField id='co' placeholder='C/O' variant='simple' />
          )}
        />
        <RowWithSelect
          label={() => (
            <Label as='label' htmlFor='city'>
              City
            </Label>
          )}
          action={() => <Select placeholder='Stockholm' />}
        />
        <RowWithTextField
          label={() => (
            <Label as='label' htmlFor='postal-code'>
              Postal code
            </Label>
          )}
          action={() => (
            <TextField
              id='postal-code'
              placeholder='Postal code'
              variant='simple'
            />
          )}
        />
        <RowWithSelect
          label={() => (
            <Label as='label' htmlFor='city'>
              Country
            </Label>
          )}
          action={() => <Select placeholder='Stockholm' />}
        />
      </Panel>
      <Panel>
        <Header
          variant='subheadline'
          colorSet='base'
          colorName='lowContrast'
          slot={() => (
            <Button variant='normalBorderless'>
              <Icon name='help' />
            </Button>
          )}
        >
          Payment information
        </Header>
        <RowWithTextField
          label={() => (
            <Label as='label' htmlFor='bank'>
              Bank
            </Label>
          )}
          action={() => (
            <TextField id='bank' placeholder='Bank' variant='simple' disabled />
          )}
        />
        <RowWithTextField
          label={() => (
            <Label as='label' htmlFor='account'>
              Account number
            </Label>
          )}
          action={() => (
            <TextField
              id='account'
              placeholder='Account number'
              variant='simple'
              disabled
            />
          )}
        />
        <Action icon={() => <Icon name='account_balance' />}>
          Change payment method
        </Action>
        <Label variant='footnote'>
          You can’t remove your connected account because you’re subscribed. To
          stop paying for PeerDB you need to delete your account.
        </Label>
      </Panel>
      <Panel>
        <Label
          variant='subheadline'
          as='h2'
          colorSet='base'
          colorName='lowContrast'
        >
          Support
        </Label>
        <div>
          <Button>Log out of all devices</Button>
          <Button>Help and support</Button>
          <Button variant='destructive'>Permanently delete account</Button>
        </div>
        <Label variant='footnote'>
          Permanently delete the account and remove access from all workspaces.
        </Label>
      </Panel>
    </LayoutMain>
  );
}
