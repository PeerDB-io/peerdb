'use client';
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
import { useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import ReactSelect from 'react-select';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { InfoPopover } from '../../../components/InfoPopover';
import { CDCConfig, TableMapRow } from '../../dto/MirrorsDTO';
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
import QRepQuery from './qrep/query';
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

const notifyErr = (msg: string, ok?: boolean) => {
  if (ok) {
    toast.success(msg, {
      position: 'bottom-center',
    });
  } else {
    toast.error(msg, {
      position: 'bottom-center',
    });
  }
};
export default function CreateMirrors() {
  const router = useRouter();
  const [mirrorName, setMirrorName] = useState<string>('');
  const [mirrorType, setMirrorType] = useState<string>('');
  const [creating, setCreating] = useState<boolean>(false);
  const [validating, setValidating] = useState<boolean>(false);
  const [config, setConfig] = useState<CDCConfig | QRepConfig>(blankCDCSetting);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [rows, setRows] = useState<TableMapRow[]>([]);
  const [qrepQuery, setQrepQuery] =
    useState<string>(`-- Here's a sample template:
    SELECT * FROM <table_name>
    WHERE <watermark_column>
    BETWEEN {{.start}} AND {{.end}}`);

  useEffect(() => {
    fetch('/api/peers', { cache: 'no-store' })
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
        <MirrorCards setMirrorType={setMirrorType} />
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
                      onChange={(val, action) =>
                        handlePeer(val, peerEnd as 'src' | 'dst', setConfig)
                      }
                      options={
                        (peerEnd === 'src'
                          ? peers.filter((peer) => peer.type == DBType.POSTGRES)
                          : peers) ?? []
                      }
                      getOptionValue={getPeerValue}
                      formatOptionLabel={getPeerLabel}
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
        {!creating && <ToastContainer />}
        {mirrorType === '' ? (
          <></>
        ) : mirrorType === 'CDC' ? (
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
            xmin={mirrorType === 'XMIN'}
          />
        )}
      </Panel>
      <Panel>
        {mirrorType && (
          <div style={styles.MirrorButtonContainer}>
            <Button
              style={styles.MirrorButtonStyle}
              variant='peer'
              disabled={creating}
              onClick={() =>
                mirrorType === 'CDC' &&
                handleValidateCDC(
                  mirrorName,
                  rows,
                  config as CDCConfig,
                  notifyErr,
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
            <Button
              style={styles.MirrorButtonStyle}
              variant='normalSolid'
              disabled={creating}
              onClick={() =>
                mirrorType === 'CDC'
                  ? handleCreateCDC(
                      mirrorName,
                      rows,
                      config as CDCConfig,
                      notifyErr,
                      setCreating,
                      listMirrorsPage
                    )
                  : handleCreateQRep(
                      mirrorName,
                      qrepQuery,
                      config as QRepConfig,
                      notifyErr,
                      setCreating,
                      listMirrorsPage,
                      mirrorType === 'XMIN' // for handling xmin specific
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
  );
}
