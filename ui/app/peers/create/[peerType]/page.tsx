'use client';
import { PeerConfig } from '@/app/dto/PeersDTO';
import BQConfig from '@/components/BigqueryConfig';
import S3ConfigForm from '@/components/S3Form';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Label } from '@/lib/Label';
import { LayoutMain, RowWithTextField } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useState } from 'react';
import ConfigForm from '../../../../components/ConfigForm';
import { handleCreate, handleValidate } from './handlers';
import { PeerSetting, getBlankSetting } from './helpers/common';
import { postgresSetting } from './helpers/pg';
import { snowflakeSetting } from './helpers/sf';

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
    const configForm = (settingList: PeerSetting[]) => (
      <ConfigForm settings={settingList} setter={setConfig} />
    );
    switch (dbType) {
      case 'POSTGRES':
        return configForm(postgresSetting);
      case 'SNOWFLAKE':
        return configForm(snowflakeSetting);
      case 'BIGQUERY':
        return <BQConfig setter={setConfig} />;
      case 'S3':
        return <S3ConfigForm setter={setConfig} />;
      default:
        return <></>;
    }
  };

  let listPeersRoute = () => {
    router.push('/peers');
  };

  return (
    <LayoutMain alignSelf='center' justifySelf='center' width='xxLarge'>
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
        {dbType && configComponentMap(dbType)}
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
    </LayoutMain>
  );
}
