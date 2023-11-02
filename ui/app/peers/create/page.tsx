'use client';
import SelectSource from '@/components/SelectSource';
import { Action } from '@/lib/Action';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain, RowWithSelect } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import Link from 'next/link';
import { useState } from 'react';

export default function CreatePeer() {
  const [peerType, setPeerType] = useState<string>('');
  return (
    <LayoutMain alignSelf='center' justifySelf='center' width='xxLarge'>
      <Panel>
        <Label variant='title3'>Select source</Label>
        <Label colorName='lowContrast'>
          Start by selecting the data source for your new peer.
        </Label>
        <Action
          icon={<Icon name='help' />}
          href='https://docs.peerdb.io/sql/commands/create-peer'
          target='_blank'
        >
          Learn about peers
        </Action>
      </Panel>
      <Panel>
        <RowWithSelect
          label={
            <Label as='label' htmlFor='source'>
              Data source
            </Label>
          }
          action={
            <SelectSource peerType={peerType} setPeerType={setPeerType} />
          }
        />
      </Panel>
      <Panel>
        <ButtonGroup>
          <Button as={Link} href='/peers'>
            Cancel
          </Button>
          <Link href={`/peers/create/${peerType}`}>
            <Button disabled={!peerType} variant='normalSolid'>
              Continue
            </Button>
          </Link>
        </ButtonGroup>
      </Panel>
    </LayoutMain>
  );
}
