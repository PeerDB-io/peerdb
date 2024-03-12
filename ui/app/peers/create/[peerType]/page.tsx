'use client';
import { PeerConfig } from '@/app/dto/PeersDTO';
import GuideForDestinationSetup from '@/app/mirrors/create/cdc/guide';
import BigqueryForm from '@/components/PeerForms/BigqueryConfig';
import ClickhouseForm from '@/components/PeerForms/ClickhouseConfig';
import PostgresForm from '@/components/PeerForms/PostgresForm';
import S3Form from '@/components/PeerForms/S3Form';
import SnowflakeForm from '@/components/PeerForms/SnowflakeForm';

import TitleCase from '@/app/utils/titlecase';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useState } from 'react';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { handleCreate, handleValidate } from './handlers';
import { clickhouseSetting } from './helpers/ch';
import { getBlankSetting } from './helpers/common';
import { postgresSetting } from './helpers/pg';
import { snowflakeSetting } from './helpers/sf';

type CreateConfigProps = {
  params: { peerType: string };
};

const notify = (msg: string, success?: boolean) => {
  if (success) {
    toast.success(msg, {
      position: 'bottom-center',
      autoClose: 1000,
    });
  } else {
    toast.error(msg, {
      position: 'bottom-center',
    });
  }
};

export default function CreateConfig({
  params: { peerType },
}: CreateConfigProps) {
  const router = useRouter();
  const blankSetting = getBlankSetting(peerType);
  const [name, setName] = useState<string>('');
  const [config, setConfig] = useState<PeerConfig>(blankSetting);
  const [loading, setLoading] = useState<boolean>(false);
  const peerLabel = peerType.toUpperCase().replaceAll('%20', ' ');
  const getDBType = () => {
    if (peerType.includes('POSTGRESQL')) {
      return 'POSTGRES';
    }
    return peerType;
  };

  const configComponentMap = (peerType: string) => {
    if (peerType.includes('POSTGRESQL')) {
      return (
        <PostgresForm
          settings={postgresSetting}
          setter={setConfig}
          type={peerType}
        />
      );
    }

    switch (peerType) {
      case 'SNOWFLAKE':
        return <SnowflakeForm settings={snowflakeSetting} setter={setConfig} />;
      case 'BIGQUERY':
        return <BigqueryForm setter={setConfig} />;
      case 'CLICKHOUSE':
        return (
          <ClickhouseForm settings={clickhouseSetting} setter={setConfig} />
        );
      case 'S3':
        return <S3Form setter={setConfig} />;
      default:
        return <></>;
    }
  };

  let listPeersRoute = () => {
    router.push('/peers');
  };

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        width: '100%',
        height: '100%',
      }}
    >
      <Panel style={{ rowGap: '0.5rem' }}>
        <Label variant='title3' as='label' style={{ marginBottom: '2rem' }}>
          Setup a {TitleCase(peerLabel)} peer
        </Label>

        <GuideForDestinationSetup createPeerType={peerLabel} />

        <RowWithTextField
          label={
            <Label>
              Name
              {
                <Tooltip
                  style={{ width: '100%' }}
                  content={'Peer name is a required field.'}
                >
                  <Label colorName='lowContrast' colorSet='destructive'>
                    *
                  </Label>
                </Tooltip>
              }
            </Label>
          }
          action={
            <TextField
              variant='simple'
              value={name}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setName(e.target.value)
              }
            />
          }
        />
        <Label colorName='lowContrast' variant='subheadline'>
          Configuration
        </Label>
        {configComponentMap(peerType)}

        <ButtonGroup>
          <Button as={Link} href='/peers/create'>
            Back
          </Button>
          <Button
            style={{ backgroundColor: 'gold' }}
            onClick={() =>
              handleValidate(getDBType(), config, notify, setLoading, name)
            }
          >
            Validate
          </Button>
          <Button
            variant='normalSolid'
            onClick={() =>
              handleCreate(
                getDBType(),
                config,
                notify,
                setLoading,
                listPeersRoute,
                name
              )
            }
          >
            Create
          </Button>
        </ButtonGroup>
        <Panel>
          {loading && (
            <Label
              colorName='lowContrast'
              colorSet='base'
              variant='subheadline'
            >
              Validating...
            </Label>
          )}
        </Panel>
      </Panel>
      <ToastContainer style={{ minWidth: '20%' }} />
    </div>
  );
}
