'use client';
import { QRepConfig } from '@/grpc_generated/flow';
import { Peer } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Label } from '@/lib/Label';
import { RowWithRadiobutton, RowWithTextField } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { RadioButton, RadioButtonGroup } from '@/lib/RadioButtonGroup';
import { TextField } from '@/lib/TextField';
import { Divider } from '@tremor/react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import { CDCConfig, TableMapRow } from '../../dto/MirrorsDTO';
import CDCConfigForm from './cdc';
import { handleCreateCDC, handleCreateQRep } from './handlers';
import { cdcSettings } from './helpers/cdc';
import { blankCDCSetting, blankQRepSetting } from './helpers/common';
import { qrepSettings } from './helpers/qrep';
import QRepConfigForm from './qrep';
import QRepQuery from './query';

export default function CreateMirrors() {
  const router = useRouter();
  const [mirrorName, setMirrorName] = useState<string>('');
  const [mirrorType, setMirrorType] = useState<string>('CDC');
  const [formMessage, setFormMessage] = useState<{ ok: boolean; msg: string }>({
    ok: true,
    msg: '',
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [config, setConfig] = useState<CDCConfig | QRepConfig>(blankCDCSetting);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [rows, setRows] = useState<TableMapRow[]>([]);
  const [sourceSchema, setSourceSchema] = useState('public');
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

  let listMirrorsPage = () => {
    router.push('/mirrors');
  };

  return (
    <div style={{ width: '60%', alignSelf: 'center', justifySelf: 'center' }}>
      <Panel>
        <Label variant='title3' as={'h2'}>
          Create a new mirror
        </Label>
        <Label colorName='lowContrast'>
          Set up a new mirror in a few easy steps.
        </Label>
      </Panel>
      <Panel>
        <Label
          as='label'
          htmlFor='mirror'
          style={{ fontWeight: 'bold', fontSize: 16, marginBottom: '0.5rem' }}
        >
          Mirror type
        </Label>
        <RadioButtonGroup onValueChange={(value) => setMirrorType(value)}>
          <div
            style={{
              display: 'flex',
              alignItems: 'start',
              marginBottom: '1rem',
            }}
          >
            <div
              style={{
                padding: '0.5rem',
                width: '35%',
                height: '20vh',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between',
                boxShadow: '2px 2px 4px rgba(0, 0, 0, 0.1)',
                backgroundColor: 'ghostwhite',
                borderRadius: '1rem',
              }}
            >
              <div>
                <RowWithRadiobutton
                  label={
                    <Label>
                      <div style={{ fontWeight: 'bold' }}>CDC</div>
                    </Label>
                  }
                  action={<RadioButton value='CDC' />}
                />
                <Label>
                  <div style={{ fontSize: 14 }}>
                    Change-data Capture or CDC refers to replication of changes
                    on the source table to the destination table, including
                    initial load.{' '}
                  </div>
                </Label>
              </div>
              <Label
                as={Link}
                style={{ color: 'teal', cursor: 'pointer' }}
                href='https://docs.peerdb.io/usecases/Real-time%20CDC/overview'
              >
                Learn more
              </Label>
            </div>

            <div
              style={{
                padding: '1rem',
                width: '35%',
                marginLeft: '0.5rem',
                marginRight: '0.5rem',
                height: '20vh',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between',
                boxShadow: '2px 2px 4px rgba(0, 0, 0, 0.1)',
                backgroundColor: 'ghostwhite',
                borderRadius: '1rem',
              }}
            >
              <div>
                <RowWithRadiobutton
                  label={
                    <Label>
                      <div style={{ fontWeight: 'bold' }}>
                        Query Replication
                      </div>
                    </Label>
                  }
                  action={<RadioButton value='Query Replication' />}
                />
                <Label>
                  <div style={{ fontSize: 14 }}>
                    Query Replication or QRep allows you to specify a set of
                    rows to be synced via a SELECT query.
                  </div>
                </Label>
              </div>
              <Label
                as={Link}
                style={{ color: 'teal', cursor: 'pointer' }}
                href='https://docs.peerdb.io/usecases/Streaming%20Query%20Replication/overview'
              >
                Learn more
              </Label>
            </div>

            <div
              style={{
                padding: '1rem',
                width: '35%',
                height: '20vh',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between',
                boxShadow: '2px 2px 4px rgba(0, 0, 0, 0.1)',
                backgroundColor: 'ghostwhite',
                borderRadius: '1rem',
              }}
            >
              <RowWithRadiobutton
                label={
                  <Label>
                    <div style={{ fontWeight: 'bold' }}>XMIN</div>
                  </Label>
                }
                action={<RadioButton value='XMIN' />}
              />
              <Label>
                <div style={{ fontSize: 14 }}>
                  XMIN mode uses the xmin system column of PostgreSQL as a
                  watermark column for replication.
                </div>
              </Label>
              <Label
                as={Link}
                style={{ color: 'teal', cursor: 'pointer' }}
                href='https://docs.peerdb.io/sql/commands/create-mirror#xmin-query-replication'
              >
                Learn more
              </Label>
            </div>
          </div>
        </RadioButtonGroup>

        <RowWithTextField
          label={<Label>Mirror Name</Label>}
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

        {mirrorType === 'Query Replication' && (
          <QRepQuery query={qrepQuery} setter={setQrepQuery} />
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
            rows={rows}
            setRows={setRows}
            setSchema={setSourceSchema}
            schema={sourceSchema}
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
                    listMirrorsPage
                  )
                : handleCreateQRep(
                    mirrorName,
                    qrepQuery,
                    config as QRepConfig,
                    setFormMessage,
                    setLoading,
                    listMirrorsPage,
                    mirrorType === 'XMIN' // for handling xmin specific
                  )
            }
          >
            Create Mirror
          </Button>
        </ButtonGroup>
      </Panel>
    </div>
  );
}
