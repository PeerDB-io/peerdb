'use client';
import { DBTypeToImageMapping } from '@/components/PeerComponent';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { QRepConfig, QRepSyncMode } from '@/grpc_generated/flow';
import { DBType, Peer } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Label } from '@/lib/Label';
import {
  RowWithRadiobutton,
  RowWithSelect,
  RowWithTextField,
} from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { RadioButton, RadioButtonGroup } from '@/lib/RadioButtonGroup';
import { Select, SelectItem } from '@/lib/Select';
import { TextField } from '@/lib/TextField';
import { Divider } from '@tremor/react';
import Image from 'next/image';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import { InfoPopover } from '../../../components/InfoPopover';
import { CDCConfig, TableMapRow } from '../../dto/MirrorsDTO';
import CDCConfigForm from './cdc';
import { handleCreateCDC, handleCreateQRep } from './handlers';
import { cdcSettings } from './helpers/cdc';
import { blankCDCSetting } from './helpers/common';
import { qrepSettings } from './helpers/qrep';
import QRepConfigForm from './qrep';
import QRepQuery from './query';

export default function CreateMirrors() {
  const router = useRouter();
  const [mirrorName, setMirrorName] = useState<string>('');
  const [mirrorType, setMirrorType] = useState<string>('');
  const [formMessage, setFormMessage] = useState<{ ok: boolean; msg: string }>({
    ok: true,
    msg: '',
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [config, setConfig] = useState<CDCConfig | QRepConfig>(blankCDCSetting);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [rows, setRows] = useState<TableMapRow[]>([]);
  const [validSource, setValidSource] = useState<boolean>(false);
  const [sourceSchema, setSourceSchema] = useState('public');
  const [qrepQuery, setQrepQuery] =
    useState<string>(`-- Here's a sample template:
    SELECT * FROM <table_name> 
    WHERE <watermark_column> 
    BETWEEN {{.start}} AND {{.end}}`);

  useEffect(() => {
    fetch('/api/peers')
      .then((res) => res.json())
      .then((res) => {
        setPeers(res);
      });

    if (mirrorType === 'Query Replication' || mirrorType === 'XMIN') {
      if (mirrorType === 'XMIN') {
        setConfig((curr) => {
          return { ...curr, setupWatermarkTableOnDestination: true };
        });
      } else
        setConfig((curr) => {
          return { ...curr, setupWatermarkTableOnDestination: false };
        });
    }
  }, [mirrorType]);

  let listMirrorsPage = () => {
    router.push('/mirrors');
  };

  const handlePeer = (val: string, peerEnd: 'src' | 'dst') => {
    const stateVal = peers.find((peer) => peer.name === val)!;
    if (peerEnd === 'dst') {
      if (stateVal.type === DBType.POSTGRES) {
        setConfig((curr) => {
          return {
            ...curr,
            cdcSyncMode: QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
            snapshotSyncMode: QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
            syncMode: QRepSyncMode.QREP_SYNC_MODE_MULTI_INSERT,
          };
        });
      } else if (
        stateVal.type === DBType.SNOWFLAKE ||
        stateVal.type === DBType.BIGQUERY
      ) {
        setConfig((curr) => {
          return {
            ...curr,
            cdcSyncMode: QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO,
            snapshotSyncMode: QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO,
            syncMode: QRepSyncMode.QREP_SYNC_MODE_STORAGE_AVRO,
          };
        });
      }
      setConfig((curr) => ({
        ...curr,
        destination: stateVal,
        destinationPeer: stateVal,
      }));
    } else {
      setConfig((curr) => ({
        ...curr,
        source: stateVal,
        sourcePeer: stateVal,
      }));
    }
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
        <RadioButtonGroup
          onValueChange={(value: string) => setMirrorType(value)}
        >
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
                height: '22vh',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between',
                border: '2px solid rgba(0, 0, 0, 0.07)',
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
                    on the source table to the target table with initial load.
                    This is recommended.
                  </div>
                </Label>
              </div>
              <Label
                as={Link}
                target='_blank'
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
                height: '22vh',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between',
                border: '2px solid rgba(0, 0, 0, 0.07)',
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
                target='_blank'
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
                height: '22vh',
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'space-between',
                border: '2px solid rgba(0, 0, 0, 0.07)',
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
                target='_blank'
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

        <RowWithSelect
          label={
            <Label>
              Source Peer
              {RequiredIndicator(true)}
            </Label>
          }
          action={
            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'center',
              }}
            >
              <Select
                placeholder='Select the source peer'
                onValueChange={(val) => handlePeer(val, 'src')}
              >
                {(
                  peers.filter((peer) => peer.type == DBType.POSTGRES) ?? []
                ).map((peer, id) => {
                  return (
                    <SelectItem key={id} value={peer.name}>
                      <div style={{ display: 'flex', alignItems: 'center' }}>
                        <div style={{ width: '5%', height: '5%' }}>
                          <Image
                            src={DBTypeToImageMapping(peer.type)}
                            alt='me'
                            width={500}
                            height={500}
                            style={{ objectFit: 'cover' }}
                          />
                        </div>
                        <div style={{ marginLeft: '1rem' }}>{peer.name}</div>
                      </div>
                    </SelectItem>
                  );
                })}
              </Select>
              <InfoPopover
                tips={
                  'The peer from which we will be replicating data. Ensure the prerequisites for this peer are met.'
                }
                link={
                  'https://docs.peerdb.io/usecases/Real-time%20CDC/postgres-to-snowflake#prerequisites'
                }
              />
            </div>
          }
        />

        <RowWithSelect
          label={
            <Label>
              Destination Peer
              {RequiredIndicator(true)}
            </Label>
          }
          action={
            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'center',
              }}
            >
              <Select
                placeholder='Select the destination peer'
                onValueChange={(val) => handlePeer(val, 'dst')}
              >
                {(peers ?? []).map((peer, id) => {
                  return (
                    <SelectItem key={id} value={peer.name}>
                      <div style={{ display: 'flex', alignItems: 'center' }}>
                        <div style={{ width: '5%', height: '5%' }}>
                          <Image
                            src={DBTypeToImageMapping(peer.type)}
                            alt='me'
                            width={500}
                            height={500}
                            style={{ objectFit: 'cover' }}
                          />
                        </div>
                        <div style={{ marginLeft: '1rem' }}>{peer.name}</div>
                      </div>
                    </SelectItem>
                  );
                })}
              </Select>
              <InfoPopover
                tips={
                  'The peer from which we will be replicating data. Ensure the prerequisites for this peer are met.'
                }
                link={
                  'https://docs.peerdb.io/usecases/Real-time%20CDC/postgres-to-snowflake#prerequisites'
                }
              />
            </div>
          }
        />
        <Divider style={{ marginTop: '1rem', marginBottom: '1rem' }} />

        {mirrorType === 'Query Replication' && (
          <QRepQuery query={qrepQuery} setter={setQrepQuery} />
        )}

        {mirrorType && (
          <Label
            as='label'
            style={{ marginTop: '1rem' }}
            colorName='lowContrast'
          >
            Configuration
          </Label>
        )}
        {!loading && formMessage.msg.length > 0 && (
          <Label
            colorName='lowContrast'
            colorSet={formMessage.ok ? 'positive' : 'destructive'}
            variant='subheadline'
          >
            {formMessage.msg}
          </Label>
        )}
        {mirrorType === '' ? (
          <></>
        ) : mirrorType === 'CDC' ? (
          <CDCConfigForm
            settings={cdcSettings}
            mirrorConfig={config as CDCConfig}
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
            setter={setConfig}
            xmin={mirrorType === 'XMIN'}
          />
        )}
      </Panel>
      <Panel>
        {mirrorType && (
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
        )}
      </Panel>
    </div>
  );
}
