'use client';
import { QRepConfig } from '@/grpc_generated/flow';
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
import { CDCConfig, TableMapRow } from '../types';
import CDCConfigForm from './cdc';
import { handleCreateCDC, handleCreateQRep } from './handlers';
import { cdcSettings } from './helpers/cdc';
import { blankCDCSetting, blankQRepSetting } from './helpers/common';
import { qrepSettings } from './helpers/qrep';
import QRepConfigForm from './qrep';
import QRepQuery from './query';
import TableMapping from './tablemapping';

export const dynamic = 'force-dynamic';

export default function CreateMirrors() {
  const router = useRouter();
  const [mirrorName, setMirrorName] = useState<string>('');
  const [mirrorType, setMirrorType] = useState<
    'CDC' | 'Query Replication' | 'XMIN'
  >('CDC');
  const [formMessage, setFormMessage] = useState<{ ok: boolean; msg: string }>({
    ok: true,
    msg: '',
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [config, setConfig] = useState<CDCConfig | QRepConfig>(blankCDCSetting);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [rows, setRows] = useState<TableMapRow[]>([
    { source: '', destination: '' },
  ]);
  const [qrepQuery, setQrepQuery] = useState<string>('');

  useEffect(() => {
    fetch('/api/peers')
      .then((res) => res.json())
      .then((res) => {
        setPeers(res);
      });

    if (mirrorType === 'Query Replication' || mirrorType === 'XMIN') {
      setConfig(blankQRepSetting);
      if (mirrorType === 'XMIN') {
        setConfig((curr) => {
          return { ...curr, setupWatermarkTableOnDestination: true };
        });
      } else
        setConfig((curr) => {
          return { ...curr, setupWatermarkTableOnDestination: false };
        });
    } else setConfig(blankCDCSetting);
  }, [mirrorType]);

  let listPeersPage = () => {
    router.push('/peers');
  };

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
            <Select
              placeholder='Select mirror type'
              onValueChange={(value) =>
                setMirrorType(value as 'CDC' | 'Query Replication')
              }
              defaultValue={mirrorType}
            >
              <SelectItem value='CDC'>CDC</SelectItem>
              <SelectItem value='Query Replication'>
                Query Replication
              </SelectItem>
              <SelectItem value='XMIN'>XMIN</SelectItem>
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

        {mirrorType === 'CDC' ? (
          <TableMapping rows={rows} setRows={setRows} />
        ) : (
          mirrorType != 'XMIN' && (
            <QRepQuery query={qrepQuery} setter={setQrepQuery} />
          )
        )}

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
        {mirrorType === 'CDC' ? (
          <CDCConfigForm
            settings={cdcSettings}
            mirrorConfig={config as CDCConfig}
            peers={peers}
            setter={setConfig}
          />
        ) : (
          <QRepConfigForm
            settings={qrepSettings}
            mirrorConfig={config as QRepConfig}
            peers={peers}
            setter={setConfig}
            xmin={mirrorType === 'XMIN'}
          />
        )}
      </Panel>
      <Panel>
        <ButtonGroup className='justify-end'>
          <Button as={Link} href='/mirrors'>
            Cancel
          </Button>
          <Button
            variant='normalSolid'
            onClick={() =>
              mirrorType === 'CDC'
                ? handleCreateCDC(
                    mirrorName,
                    rows,
                    config as CDCConfig,
                    setFormMessage,
                    setLoading,
                    listPeersPage
                  )
                : handleCreateQRep(
                    mirrorName,
                    qrepQuery,
                    config as QRepConfig,
                    setFormMessage,
                    setLoading,
                    listPeersPage,
                    mirrorType === 'XMIN' // for handling xmin specific
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
