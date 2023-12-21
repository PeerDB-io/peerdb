'use client';
import { PeerConfig } from '@/app/dto/PeersDTO';
import BigqueryForm from '@/components/PeerForms/BigqueryConfig';
import PostgresForm from '@/components/PeerForms/PostgresForm';
import ClickhouseForm from '@/components/PeerForms/ClickhouseConfig';
import S3Form from '@/components/PeerForms/S3Form';
import SnowflakeForm from '@/components/PeerForms/SnowflakeForm';
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
import { handleCreate, handleValidate } from './handlers';
import { getBlankSetting } from './helpers/common';
import { postgresSetting } from './helpers/pg';
import { snowflakeSetting } from './helpers/sf';
import {clickhouseSetting} from './helpers/ch';

type CreateConfigProps = {
  params: { peerType: string };
};

export default function CreateConfig({
  params: { peerType },
}: CreateConfigProps) {
  const router = useRouter();
  const dbType = peerType;
  const blankSetting = getBlankSetting(dbType);
  const [name, setName] = useState<string>('');
  const [config, setConfig] = useState<PeerConfig>(blankSetting);
  const [formMessage, setFormMessage] = useState<{ ok: boolean; msg: string }>({
    ok: true,
    msg: '',
  });
  const [loading, setLoading] = useState<boolean>(false);
  const configComponentMap = (dbType: string) => {
    switch (dbType) {
      case 'POSTGRES':
        return <PostgresForm settings={postgresSetting} setter={setConfig} />;
      case 'SNOWFLAKE':
        return <SnowflakeForm settings={snowflakeSetting} setter={setConfig} />;
      case 'BIGQUERY':
        return <BigqueryForm setter={setConfig} />;
      case 'S3':
        return <S3Form setter={setConfig} />;
      case 'CLICKHOUSE':
        return <ClickhouseForm settings={clickhouseSetting} setter={setConfig} />;
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
        alignSelf: 'center',
        justifySelf: 'center',
        width: '45%',
      }}
    >
      <Panel>
        <Label variant='title3'>
          Setup a new{' '}
          {dbType.charAt(0).toUpperCase() + dbType.slice(1).toLowerCase()} peer
        </Label>
      </Panel>
      <Panel>
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
        {configComponentMap(dbType)}
      </Panel>
      <Panel>
        <ButtonGroup>
          <Button as={Link} href='/peers/create'>
            Back
          </Button>
          <Button
            style={{ backgroundColor: 'gold' }}
            onClick={() =>
              handleValidate(dbType, config, setFormMessage, setLoading, name)
            }
          >
            Validate
          </Button>
          <Button
            variant='normalSolid'
            onClick={() =>
              handleCreate(
                dbType,
                config,
                setFormMessage,
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
          {!loading && formMessage.msg.length > 0 && (
            <Label
              colorName='lowContrast'
              colorSet={formMessage.ok === true ? 'positive' : 'destructive'}
              variant='subheadline'
            >
              {formMessage.msg}
            </Label>
          )}
        </Panel>
      </Panel>
    </div>
  );
}
