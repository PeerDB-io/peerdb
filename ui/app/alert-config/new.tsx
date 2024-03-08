import { Button } from '@/lib/Button';
import { TextField } from '@/lib/TextField';
import Image from 'next/image';
import { Dispatch, SetStateAction, useState } from 'react';
import ReactSelect from 'react-select';
import { PulseLoader } from 'react-spinners';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import SelectTheme from '../styles/select';
import {
  alertConfigReqSchema,
  alertConfigType,
  emailConfigType, serviceConfigType,
  serviceTypeSchemaMap,
  slackConfigType
} from './validation';

export type ServiceType = 'slack' | 'email';

export interface AlertConfigProps {
  id?: bigint;
  serviceType: ServiceType;
  alertConfig: serviceConfigType;
  forEdit?: boolean;
}

const notifyErr = (errMsg: string) => {
  toast.error(errMsg, {
    position: 'bottom-center',
  });
};

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

  const [loading, setLoading] = useState(false);

  const handleAdd = async () => {
    if (!serviceType) {
      notifyErr('Service type must be selected');
      return;
    }

    const serviceSchema = serviceTypeSchemaMap[serviceType]
    const serviceValidity = serviceSchema.safeParse(config)
    if(!serviceValidity?.success){
      notifyErr("Invalid alert service configuration for "+ serviceType + ". " + serviceValidity.error.issues[0].message)
      return;
    }

    const serviceConfig = serviceValidity.data
    const alertConfigReq: alertConfigType = {
      id: Number(alertProps.id || -1),
      serviceType: serviceType,
      serviceConfig,
    };

    const alertReqValidity = alertConfigReqSchema.safeParse(alertConfigReq);
    if (!alertReqValidity.success) {
      notifyErr(alertReqValidity.error.issues[0].message);
      return;
    }

    setLoading(true);
    if (alertProps.forEdit) {
      alertConfigReq.id = Number(alertProps.id);
    }
    const createRes = await fetch('/api/alert-config', {
      method: alertProps.forEdit ? 'PUT' : 'POST',
      body: JSON.stringify(alertConfigReq),
    });

    const createStatus = await createRes.text();
    setLoading(false);
    if (createStatus !== 'success') {
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
        rowGap: '2rem',
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
