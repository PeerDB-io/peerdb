'use client';
import SelectTheme from '@/app/styles/select';
import QRepQueryTemplate from '@/app/utils/qreptemplate';
import { DBTypeToImageMapping } from '@/components/PeerComponent';
import { RequiredIndicator } from '@/components/RequiredIndicator';
import { QRepConfig } from '@/grpc_generated/flow';
import { DBType, Peer } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { RowWithSelect, RowWithTextField } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { TextField } from '@/lib/TextField';
import { Divider } from '@tremor/react';
import Image from 'next/image';
import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';
import { useEffect, useState } from 'react';
import ReactSelect from 'react-select';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { InfoPopover } from '../../../components/InfoPopover';
import PeerDBCodeEditor from '../../../components/PeerDBEditor';
import { CDCConfig, MirrorType, TableMapRow } from '../../dto/MirrorsDTO';
import CDCConfigForm from './cdc/cdc';
import {
  handleCreateCDC,
  handleCreateQRep,
  handlePeer,
  handleValidateCDC,
} from './handlers';
import { cdcSettings } from './helpers/cdc';
import { blankCDCSetting } from './helpers/common';
import { qrepSettings } from './helpers/qrep';
import MirrorCards from './mirrorcards';
import QRepConfigForm from './qrep/qrep';
import * as styles from './styles';

function getPeerValue(peer: Peer) {
  return peer.name;
}

function getPeerLabel(peer: Peer) {
  return (
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
  );
}

export default function CreateMirrors() {
  const router = useRouter();
  const mirrorParam = useSearchParams();
  const mirrorTypeParam = mirrorParam.get('type');
  const [mirrorName, setMirrorName] = useState<string>('');
  const [mirrorType, setMirrorType] = useState<MirrorType>(
    (mirrorTypeParam as MirrorType) ?? MirrorType.CDC
  );
  const [creating, setCreating] = useState<boolean>(false);
  const [validating, setValidating] = useState<boolean>(false);
  const [config, setConfig] = useState<CDCConfig | QRepConfig>(blankCDCSetting);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [rows, setRows] = useState<TableMapRow[]>([]);
  const [qrepQuery, setQrepQuery] = useState<string>(QRepQueryTemplate);

  useEffect(() => {
    fetch('/api/peers', { cache: 'no-store' })
      .then((res) => res.json())
      .then((res) => {
        setPeers(res);
      });

    if (mirrorType != MirrorType.CDC) {
      if (mirrorType === MirrorType.XMin) {
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

  return (
    <div
      style={{
        width: '100%',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      }}
    >
      <div style={{ width: '70%' }}>
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
          <MirrorCards mirrorType={mirrorType} setMirrorType={setMirrorType} />
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
          {['src', 'dst'].map((peerEnd, index) => {
            return (
              <RowWithSelect
                key={index}
                label={
                  <Label>
                    {peerEnd === 'src' ? 'Source Peer' : 'Destination Peer'}
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
                    <div style={{ width: '100%' }}>
                      <ReactSelect
                        placeholder={`Select the ${
                          peerEnd === 'src' ? 'source' : 'destination'
                        } peer`}
                        onChange={(val) =>
                          handlePeer(val, peerEnd as 'src' | 'dst', setConfig)
                        }
                        options={
                          (peerEnd === 'src'
                            ? peers.filter(
                                (peer) => peer.type == DBType.POSTGRES
                              )
                            : peers) ?? []
                        }
                        getOptionValue={getPeerValue}
                        formatOptionLabel={getPeerLabel}
                        theme={SelectTheme}
                      />
                    </div>
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
            );
          })}

          <Divider style={{ marginTop: '1rem', marginBottom: '1rem' }} />

          {mirrorType === MirrorType.QRep && (
            <div
              style={{
                display: 'flex',
                flexDirection: 'column',
                rowGap: '1rem',
              }}
            >
              <Label variant='subheadline'>Replication Query</Label>
              <Label>
                Write a query whose results will be replicated to a target
                table.
                <br></br>
                In most cases, you will require a watermark table and a
                watermark column in that table.
                <br></br>
                <Link
                  style={{
                    color: 'teal',
                    cursor: 'pointer',
                    width: 'fit-content',
                  }}
                  target='_blank'
                  href={
                    'https://docs.peerdb.io/usecases/Streaming%20Query%20Replication/postgres-to-snowflake#step-2-set-up-mirror-to-transform-and-sync-data'
                  }
                >
                  What does that mean ?
                </Link>
              </Label>
              <PeerDBCodeEditor code={qrepQuery} setter={setQrepQuery} />
            </div>
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
          {!creating && <ToastContainer />}
          {!mirrorType ? (
            <></>
          ) : mirrorType === MirrorType.CDC ? (
            <CDCConfigForm
              settings={cdcSettings}
              mirrorConfig={config as CDCConfig}
              setter={setConfig}
              rows={rows}
              setRows={setRows}
            />
          ) : (
            <QRepConfigForm
              settings={qrepSettings}
              mirrorConfig={config as QRepConfig}
              setter={setConfig}
              xmin={mirrorType === MirrorType.XMin}
            />
          )}
        </Panel>
        <Panel>
          {mirrorType && (
            <div style={styles.MirrorButtonContainer}>
              {mirrorType === MirrorType.CDC && (
                <Button
                  style={styles.MirrorButtonStyle}
                  variant='peer'
                  disabled={creating}
                  onClick={() =>
                    handleValidateCDC(
                      mirrorName,
                      rows,
                      config as CDCConfig,
                      setValidating
                    )
                  }
                >
                  {validating ? (
                    <ProgressCircle variant='determinate_progress_circle' />
                  ) : (
                    <>
                      <Icon name='checklist' /> Validate
                    </>
                  )}
                </Button>
              )}
              <Button
                style={styles.MirrorButtonStyle}
                variant='normalSolid'
                disabled={creating}
                onClick={() =>
                  mirrorType === MirrorType.CDC
                    ? handleCreateCDC(
                        mirrorName,
                        rows,
                        config as CDCConfig,
                        setCreating,
                        listMirrorsPage
                      )
                    : handleCreateQRep(
                        mirrorName,
                        qrepQuery,
                        config as QRepConfig,
                        setCreating,
                        listMirrorsPage,
                        mirrorType === MirrorType.XMin // for handling xmin specific
                      )
                }
              >
                {creating ? (
                  <ProgressCircle variant='determinate_progress_circle' />
                ) : (
                  <>
                    <Icon name='add' /> Create Mirror
                  </>
                )}
              </Button>
            </div>
          )}
        </Panel>
      </div>
    </div>
  );
}
