"use client"
import { Action } from '@/lib/Action';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain, RowWithSelect } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import SelectSource from '../../../components/SelectSource';
import { useState } from 'react';
import { useRouter } from 'next/navigation'
import Link from 'next/link';

export default function CreatePeer() {
  const [peerType, setPeerType] = useState<string>("")
  const router = useRouter();
  return (
    <LayoutMain alignSelf='center' justifySelf='center' width='xxLarge'>
      <Panel>
        <Label variant='title3'>Select source</Label>
        <Label colorName='lowContrast'>
          Start by selecting the data source for your new peer.
        </Label>
        <Action icon={<Icon name='help' />} href='https://docs.peerdb.io/sql/commands/create-peer' target='_blank'>Learn about peers</Action>
      </Panel>
      <Panel>
        <Label colorName='lowContrast' variant='subheadline'>
          Source
        </Label>
        <RowWithSelect
          label={
            <Label as='label' htmlFor='source'>
              Data source
            </Label>
          }
            action={<SelectSource peerType={peerType} setPeerType={setPeerType}/>}
        />
      </Panel>
      <Panel>
        <ButtonGroup>
          <Button as={Link} href='/peers'>Cancel</Button>
          <Button disabled={!peerType} onClick={()=>{
            router.push(`/peers/create/details?dbtype=${peerType}`)
          }} variant='normalSolid'>Continue</Button>
        </ButtonGroup>
      </Panel>
    </LayoutMain>
  );
}
