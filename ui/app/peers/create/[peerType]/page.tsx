'use client';
import { PeerConfig } from '@/app/dto/PeersDTO';
import GuideForDestinationSetup from '@/app/mirrors/create/cdc/guide';
import BigqueryForm from '@/components/PeerForms/BigqueryConfig';
import ClickhouseForm from '@/components/PeerForms/ClickhouseConfig';
import KafkaForm from '@/components/PeerForms/KafkaConfig';
import PostgresForm from '@/components/PeerForms/PostgresForm';
import PubSubForm from '@/components/PeerForms/PubSubConfig';
import S3Form from '@/components/PeerForms/S3Form';
import SnowflakeForm from '@/components/PeerForms/SnowflakeForm';

import { notifyErr } from '@/app/utils/notify';
import TitleCase from '@/app/utils/titlecase';
import EventhubsForm from '@/components/PeerForms/Eventhubs/EventhubGroupConfig';
import { EventHubGroupConfig } from '@/grpc_generated/peers';
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
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { handleCreate, handleValidate } from './handlers';
import { clickhouseSetting } from './helpers/ch';
import { getBlankSetting } from './helpers/common';
import { postgresSetting } from './helpers/pg';
import { snowflakeSetting } from './helpers/sf';

type CreateConfigProps = {
  params: { peerType: string };
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
      case 'KAFKA':
        return <KafkaForm setter={setConfig} />;
      case 'PUBSUB':
        return <PubSubForm setter={setConfig} />;
      case 'EVENTHUBS':
        return (
          <EventhubsForm
            groupConfig={config as EventHubGroupConfig}
            setter={setConfig}
          />
        );
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

        <div style={{ minWidth: '40vw' }}>{configComponentMap(peerType)}</div>

        <ButtonGroup>
          <Button as={Link} href='/peers/create'>
            Back
          </Button>
          <Button
            style={{ backgroundColor: 'wheat' }}
            onClick={() =>
              handleValidate(getDBType(), config, notifyErr, setLoading, name)
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
                notifyErr,
                setLoading,
                listPeersRoute,
                name
              )
            }
          >
            Create peer
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
