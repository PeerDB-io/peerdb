'use client';
import { PeerConfig } from '@/app/dto/PeersDTO';
import GuideForDestinationSetup from '@/app/mirrors/create/cdc/guide';
import BigqueryForm from '@/components/PeerForms/BigqueryConfig';
import ClickHouseForm from '@/components/PeerForms/ClickhouseConfig';
import KafkaForm from '@/components/PeerForms/KafkaConfig';
import PostgresForm from '@/components/PeerForms/PostgresForm';
import PubSubForm from '@/components/PeerForms/PubSubConfig';
import S3Form from '@/components/PeerForms/S3Form';
import SnowflakeForm from '@/components/PeerForms/SnowflakeForm';

import { notifyErr } from '@/app/utils/notify';
import TitleCase from '@/app/utils/titlecase';
import ElasticsearchConfigForm from '@/components/PeerForms/ElasticsearchConfigForm';
import EventhubsForm from '@/components/PeerForms/Eventhubs/EventhubGroupConfig';
import {
  ElasticsearchConfig,
  EventHubGroupConfig,
} from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';
import { useState } from 'react';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { handleCreate, handleValidate } from './handlers';
import { clickhouseSetting } from './helpers/ch';
import { getBlankSetting } from './helpers/common';
import { postgresSetting } from './helpers/pg';
import { snowflakeSetting } from './helpers/sf';
import { peerNameSchema } from './schema';

type CreateConfigProps = {
  params: { peerType: string };
};

// when updating a peer we get ?update=<peer_name>
export default function CreateConfig({
  params: { peerType },
}: CreateConfigProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const peerName = searchParams.get('update');
  const blankSetting = getBlankSetting(peerType);
  const [name, setName] = useState<string>(
    peerName ?? searchParams.get('name') ?? ''
  );
  const [config, setConfig] = useState<PeerConfig>(blankSetting);
  const [loading, setLoading] = useState<boolean>(false);
  const peerLabel = peerType.toUpperCase().replaceAll('%20', ' ');
  const [nameValidityMessage, setNameValidityMessage] = useState<string>('');

  const getDBType = () => {
    if (
      peerType.includes('POSTGRES') ||
      peerType.includes('TEMBO') ||
      peerType.includes('NEON') ||
      peerType.includes('SUPABASE')
    ) {
      return 'POSTGRES';
    }
    if (peerType === 'CONFLUENT' || peerType === 'REDPANDA') {
      return 'KAFKA';
    }
    return peerType;
  };

  const configComponentMap = (peerType: string) => {
    switch (getDBType()) {
      case 'POSTGRES':
        return (
          <PostgresForm
            settings={postgresSetting}
            setter={setConfig}
            config={config}
            type={peerType}
          />
        );
      case 'SNOWFLAKE':
        return <SnowflakeForm settings={snowflakeSetting} setter={setConfig} />;
      case 'BIGQUERY':
        return <BigqueryForm setter={setConfig} />;
      case 'CLICKHOUSE':
        return (
          <ClickHouseForm settings={clickhouseSetting} setter={setConfig} />
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
      case 'ELASTICSEARCH':
        return (
          <ElasticsearchConfigForm
            config={config as ElasticsearchConfig}
            setter={setConfig}
          />
        );
      default:
        return <></>;
    }
  };

  const validatePeerName = (e: React.ChangeEvent<HTMLInputElement>) => {
    const currentlyTypedPeerName = e.target.value;
    if (currentlyTypedPeerName !== '') {
      const peerNameValid = peerNameSchema.safeParse(currentlyTypedPeerName);
      if (!peerNameValid.success) {
        setNameValidityMessage(peerNameValid.error.errors[0].message);
      } else {
        setNameValidityMessage('');
      }
    }

    setName(currentlyTypedPeerName);
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
              {peerName === null && (
                <Tooltip
                  style={{ width: '100%' }}
                  content='Peer name is a required field.'
                >
                  <Label colorName='lowContrast' colorSet='destructive'>
                    *
                  </Label>
                </Tooltip>
              )}
            </Label>
          }
          action={
            <TextField
              variant='simple'
              value={peerName ?? name}
              onChange={peerName === null ? validatePeerName : undefined}
              readOnly={peerName !== null}
            />
          }
        />
        {nameValidityMessage && (
          <Label
            variant='footnote'
            colorName='lowContrast'
            colorSet='destructive'
          >
            {nameValidityMessage}
          </Label>
        )}
        {peerName && (
          <Label colorName='lowContrast' colorSet='destructive'>
            Warning: Changes will only be reflected if you pause and resume the
            mirrors using this peer.
          </Label>
        )}
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
            {peerName ? 'Update peer' : 'Create peer'}
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
