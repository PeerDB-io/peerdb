'use client';
import { FlowConnectionConfigs } from '@/grpc_generated/flow';
import { Peer } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Label } from '@/lib/Label';
import { LayoutMain, RowWithSelect, RowWithTextField } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { Select, SelectItem } from '@/lib/Select';
import { TextField } from '@/lib/TextField';
import { Divider } from '@tremor/react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import { TableMapRow } from '../types';
import MirrorConfig from './config';
import { handleCreate } from './handlers';
import { cdcSettings } from './helpers/cdc';
import { blankCDCSetting } from './helpers/common';
import TableMapping from './tablemapping';

export default function CreateMirrors() {
  const router = useRouter();
  const [mirrorName, setMirrorName] = useState<string>('');
  const [mirrorType, setMirrorType] = useState<'CDC' | 'QREP'>('CDC');
  const [formMessage, setFormMessage] = useState<{ ok: boolean; msg: string }>({
    ok: true,
    msg: '',
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [config, setConfig] = useState<FlowConnectionConfigs>(blankCDCSetting);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [rows, setRows] = useState<TableMapRow[]>([
    { source: '', destination: '' },
  ]);
  useEffect(() => {
    fetch('/api/peers')
      .then((res) => res.json())
      .then((res) => {
        setPeers(res.peers);
      });
  }, []);
  return (
    <LayoutMain width='xxLarge' alignSelf='center' justifySelf='center'>
      <Panel>
        <Label variant='title3' as={'h2'}>
          Create a new mirror
        </Label>
        <Label colorName='lowContrast'>
          Set up a new mirror in a few easy steps.
        </Label>
      </Panel>
      <Panel>
        <RowWithSelect
          label={
            <Label as='label' htmlFor='mirror'>
              Mirror type
            </Label>
          }
          action={
            <Select placeholder='Select mirror type' defaultValue={mirrorType}>
              <SelectItem value='CDC'>CDC</SelectItem>
            </Select>
          }
        />
        <RowWithTextField
          label={<Label>Name</Label>}
          action={
            <TextField
              variant='simple'
              value={mirrorName}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setMirrorName(e.target.value)
              }
            />
          }
        />
        <Divider style={{ marginTop: '1rem', marginBottom: '1rem' }} />
        <Label colorName='lowContrast'>Table Mapping</Label>
        <TableMapping rows={rows} setRows={setRows} />
        <Divider style={{ marginTop: '1rem', marginBottom: '1rem' }} />
        <Label colorName='lowContrast'>Configuration</Label>
        {!loading && formMessage.msg.length > 0 && (
          <Label
            colorName='lowContrast'
            colorSet={formMessage.ok ? 'positive' : 'destructive'}
            variant='subheadline'
          >
            {formMessage.msg}
          </Label>
        )}
        <MirrorConfig
          settings={cdcSettings}
          mirrorConfig={config}
          peers={peers}
          setter={setConfig}
        />
      </Panel>
      <Panel>
        <ButtonGroup className='justify-end'>
          <Button as={Link} href='/mirrors'>
            Cancel
          </Button>
          <Button
            variant='normalSolid'
            onClick={() =>
              handleCreate(
                mirrorName,
                rows,
                config,
                setFormMessage,
                setLoading,
                router
              )
            }
          >
            Create Mirror
          </Button>
        </ButtonGroup>
      </Panel>
    </LayoutMain>
  );
}
