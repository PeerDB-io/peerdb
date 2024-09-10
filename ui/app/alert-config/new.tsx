import { PostAlertConfigRequest } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label/Label';
import { TextField } from '@/lib/TextField';
import Image from 'next/image';
import Link from 'next/link';
import { Dispatch, SetStateAction, useState } from 'react';
import ReactSelect from 'react-select';
import { PulseLoader } from 'react-spinners';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import SelectTheme from '../styles/select';
import { notifyErr } from '../utils/notify';
import {
  alertConfigReqSchema,
  alertConfigType,
  emailConfigType,
  serviceConfigType,
  serviceTypeSchemaMap,
  slackConfigType,
} from './validation';

export type ServiceType = 'slack' | 'email';

export interface AlertConfigProps {
  id?: number;
  serviceType: ServiceType;
  alertConfig: serviceConfigType;
  forEdit?: boolean;
  alertForMirrors?: string[];
}

function ConfigLabel(data: { label: string; value: string }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center' }}>
      <Image
        src={`/images/${data.value}.png`}
        alt={data.value}
        height={20}
        width={20}
        style={{
          marginRight: '5px',
        }}
      />
      {data.label}
    </div>
  );
}

function getSlackProps(
  config: slackConfigType,
  setConfig: Dispatch<SetStateAction<slackConfigType>>
) {
  return (
    <>
      <div>
        <p>Authorisation Token</p>
        <TextField
          key={'auth_token'}
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          placeholder='Auth Token'
          value={config.auth_token}
          onChange={(e) => {
            setConfig((previous) => ({
              ...previous,
              auth_token: e.target.value,
            }));
          }}
        />
      </div>
      <div>
        <p>Channel IDs</p>
        <TextField
          key={'channel_ids'}
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          placeholder='Comma separated'
          value={config.channel_ids?.join(',')}
          onChange={(e) => {
            setConfig((previous) => ({
              ...previous,
              channel_ids: e.target.value.split(','),
            }));
          }}
        />
      </div>
      <div>
        <p>Members</p>
        <Label as='label' style={{ fontSize: 14 }}>
          Slack usernames to tag in the channel for these alerts. If left empty,
          pings the whole channel
        </Label>
        <TextField
          key={'members'}
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          placeholder='Comma separated'
          value={config.members?.join(',')}
          onChange={(e) => {
            setConfig((previous) => ({
              ...previous,
              members: e.target.value.split(','),
            }));
          }}
        />
      </div>
    </>
  );
}

function getEmailProps(
  config: emailConfigType,
  setConfig: Dispatch<SetStateAction<emailConfigType>>
) {
  return (
    <>
      <div>
        <p>Email Addresses</p>
        <TextField
          key={'email_addresses'}
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          placeholder='Comma separated'
          value={config.email_addresses?.join(',')}
          onChange={(e) => {
            setConfig((previous) => ({
              ...previous,
              email_addresses: e.target.value.split(','),
            }));
          }}
        />
      </div>
    </>
  );
}
function getServiceFields<T extends serviceConfigType>(
  serviceType: ServiceType,
  config: T,
  setConfig: Dispatch<SetStateAction<T>>
) {
  switch (serviceType) {
    case 'email':
      return getEmailProps(
        config as emailConfigType,
        setConfig as Dispatch<SetStateAction<emailConfigType>>
      );
    case 'slack': {
      return getSlackProps(
        config as slackConfigType,
        setConfig as Dispatch<SetStateAction<slackConfigType>>
      );
    }
  }
}

export function NewConfig(alertProps: AlertConfigProps) {
  const [serviceType, setServiceType] = useState<ServiceType>(
    alertProps.serviceType
  );

  const [config, setConfig] = useState<serviceConfigType>(
    alertProps.alertConfig
  );

  const [alertForMirrors, setAlertForMirrors] = useState<string[]>(
    alertProps.alertForMirrors || []
  );

  const [loading, setLoading] = useState(false);

  const handleAdd = async () => {
    if (!serviceType) {
      notifyErr('Service type must be selected');
      return;
    }

    const serviceSchema = serviceTypeSchemaMap[serviceType];
    const serviceValidity = serviceSchema.safeParse(config);
    if (!serviceValidity?.success) {
      notifyErr(
        `Invalid alert service configuration for ${
          serviceType
        }. ${serviceValidity.error.issues[0].message}`
      );
      return;
    }

    const serviceConfig = serviceValidity.data;
    const alertConfigReq: alertConfigType = {
      id: Number(alertProps.id || -1),
      serviceType,
      serviceConfig,
      alertForMirrors,
    };

    const alertReqValidity = alertConfigReqSchema.safeParse(alertConfigReq);
    if (!alertReqValidity.success) {
      notifyErr(alertReqValidity.error.issues[0].message);
      return;
    }

    const alertConfigProtoReq: PostAlertConfigRequest = {
      config: {
        id: alertConfigReq.id || -1,
        serviceType: alertConfigReq.serviceType,
        serviceConfig: JSON.stringify(alertConfigReq.serviceConfig),
        alertForMirrors:
          alertConfigReq.alertForMirrors?.filter(
            (mirror) => mirror && mirror.trim() !== ''
          ) || [],
      },
    };

    setLoading(true);
    const createRes = await fetch('/api/v1/alerts/config', {
      method: 'POST',
      body: JSON.stringify(alertConfigProtoReq),
    });

    setLoading(false);
    if (!createRes.ok) {
      notifyErr('Something went wrong. Please try again');
      return;
    }

    window.location.reload();
  };
  const ServiceFields = getServiceFields(serviceType, config, setConfig);
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        rowGap: '1.5rem',
        marginTop: '3rem',
        width: '40%',
      }}
    >
      <div style={{ width: '50%' }}>
        <p style={{ marginBottom: '0.5rem' }}>Alert Provider</p>
        <ReactSelect
          key={'serviceType'}
          options={[
            {
              value: 'slack',
              label: 'Slack',
            },
            {
              value: 'email',
              label: 'Email',
            },
          ]}
          placeholder='Select provider'
          defaultValue={{
            value: serviceType,
            label: serviceType.charAt(0).toUpperCase() + serviceType.slice(1),
          }}
          formatOptionLabel={ConfigLabel}
          onChange={(val, _) => val && setServiceType(val.value as ServiceType)}
          theme={SelectTheme}
        />
      </div>
      {serviceType === 'slack' && (
        <Label
          as={Link}
          target='_blank'
          href='https://docs.peerdb.io/features/alerting/slack-alerting'
          style={{ color: 'teal', cursor: 'pointer', width: 'fit-content' }}
        >
          How to setup Slack alerting
        </Label>
      )}
      <div>
        <p>Slot Lag Alert Threshold (in GB)</p>
        <TextField
          key={'slot_lag_mb_alert_threshold'}
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          type={'number'}
          placeholder='optional'
          value={config.slot_lag_mb_alert_threshold / 1000}
          onChange={(e) =>
            setConfig((previous) => ({
              ...previous,
              slot_lag_mb_alert_threshold: e.target.valueAsNumber * 1000,
            }))
          }
        />
      </div>
      <div>
        <p>Open Connections Alert Threshold</p>
        <TextField
          key={'open_connections_alert_threshold'}
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          type={'number'}
          placeholder='optional'
          value={config.open_connections_alert_threshold}
          onChange={(e) =>
            setConfig((previous) => ({
              ...previous,
              open_connections_alert_threshold: e.target.valueAsNumber,
            }))
          }
        />
      </div>
      <div>
        <p>
          Alert only for these mirrors (leave empty to alert for all mirrors)
        </p>
        <TextField
          key={'alert_for_mirrors'}
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          placeholder='Comma separated'
          value={alertForMirrors.join(',')}
          onChange={(e) => setAlertForMirrors(e.target.value.split(','))}
        />
      </div>
      {ServiceFields}
      <Button
        style={{ marginTop: '1rem', width: '20%', height: '2.5rem' }}
        onClick={handleAdd}
        variant='normalSolid'
      >
        {loading ? (
          <PulseLoader color='white' size={10} />
        ) : alertProps.forEdit ? (
          'Update'
        ) : (
          'Create'
        )}
      </Button>
      <ToastContainer />
    </div>
  );
}

export default NewConfig;
