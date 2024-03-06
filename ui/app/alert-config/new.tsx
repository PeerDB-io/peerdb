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
  emailConfigType,
  serviceConfigType,
  slackConfigType,
} from './validation';

export interface AlertConfigProps {
  id?: bigint;
  serviceType: string;
  authToken: string;
  channelIdString: string;
  slotLagGBAlertThreshold: number;
  openConnectionsAlertThreshold: number;
  forEdit?: boolean;
}

export type serviceType2 = 'slack' | 'email';

export interface AlertConfigProps2 {
  id?: bigint;
  serviceType: serviceType2;
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
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          placeholder='Auth Token'
          value={config.auth_token}
          onChange={(e) => {
            setConfig((value) => {
              value.auth_token = e.target.value;
              return value;
            });
          }}
        />
      </div>
      <div>
        <p>Channel IDs</p>
        <TextField
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          placeholder='Comma separated'
          value={config.channel_ids?.join(',')}
          onChange={(e) => {
            setConfig((value) => {
              value.channel_ids = e.target.value.split(',');
              return value;
            });
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
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          placeholder='Comma separated'
          value={config.email_addresses?.join(',')}
          onChange={(e) => {
            setConfig((value) => {
              value.email_addresses = e.target.value.split(',');
              return value;
            });
          }}
        />
      </div>
    </>
  );
}
function getExtraProps<T extends serviceConfigType>(
  serviceType: serviceType2,
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

export function NewConfig(alertProps: AlertConfigProps2) {
  const [serviceType, setServiceType] = useState<serviceType2>(
    alertProps.serviceType
  );

  const [config, setConfig] = useState<serviceConfigType>(
    alertProps.alertConfig
  );
  const [slotLagGBAlertThreshold, setSlotLagGBAlertThreshold] =
    useState<number>(alertProps.alertConfig.slot_lag_mb_alert_threshold);
  const [openConnectionsAlertThreshold, setOpenConnectionsAlertThreshold] =
    useState<number>(alertProps.alertConfig.open_connections_alert_threshold);
  const [loading, setLoading] = useState(false);
  const handleAdd = async () => {
    if (serviceType !== 'slack') {
      notifyErr('Service Type must be selected');
      return;
    }

    const alertConfigReq: alertConfigType = {
      serviceType: serviceType,
      serviceConfig: config,
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
  const extraProps = getExtraProps(serviceType, config, setConfig);
  return (
    <form onSubmit={handleAdd}>
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
              value: 'slack',
              label: 'Slack',
            }}
            formatOptionLabel={ConfigLabel}
            onChange={(val, _) => val && setServiceType(val.value)}
            theme={SelectTheme}
          />
        </div>
        <div>
          <p>Slot Lag Alert Threshold (in GB)</p>
          <TextField
            style={{ height: '2.5rem', marginTop: '0.5rem' }}
            variant='simple'
            type={'number'}
            placeholder='optional'
            value={slotLagGBAlertThreshold}
            onChange={(e) => setSlotLagGBAlertThreshold(e.target.valueAsNumber)}
          />
        </div>
        <div>
          <p>Open Connections Alert Threshold</p>
          <TextField
            style={{ height: '2.5rem', marginTop: '0.5rem' }}
            variant='simple'
            type={'number'}
            placeholder='optional'
            value={openConnectionsAlertThreshold}
            onChange={(e) =>
              setOpenConnectionsAlertThreshold(e.target.valueAsNumber)
            }
          />
        </div>
        {getExtraProps(serviceType, config, setConfig)}
        <Button
          style={{ marginTop: '1rem', width: '20%', height: '2.5rem' }}
          onClick={handleAdd}
          type={'submit'}
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
    </form>
  );
}
//
export const NewAlertConfig = (alertProps: AlertConfigProps) => {
  const [serviceType, setServiceType] = useState<string>('slack');
  const [config, setConfig] = useState<serviceConfigType>({
    auth_token: alertProps.authToken ?? '',
    channel_ids: alertProps.channelIdString?.split(',')!,
    slot_lag_mb_alert_threshold:
      alertProps.slotLagGBAlertThreshold * 1000 || 20000,
    open_connections_alert_threshold:
      alertProps.openConnectionsAlertThreshold || 5,
  });
  const [authToken, setAuthToken] = useState<string>(alertProps.authToken);
  const [channelIdString, setChannelIdString] = useState<string>(
    alertProps.channelIdString
  );
  const [slotLagGBAlertThreshold, setSlotLagGBAlertThreshold] =
    useState<number>(alertProps.slotLagGBAlertThreshold);
  const [openConnectionsAlertThreshold, setOpenConnectionsAlertThreshold] =
    useState<number>(alertProps.openConnectionsAlertThreshold);
  const [loading, setLoading] = useState(false);
  const handleAdd = async () => {
    if (serviceType !== 'slack') {
      notifyErr('Service Type must be selected');
      return;
    }

    const alertConfigReq: alertConfigType = {
      serviceType: serviceType,
      serviceConfig: {
        auth_token: authToken ?? '',
        channel_ids: channelIdString?.split(',')!,
        slot_lag_mb_alert_threshold: slotLagGBAlertThreshold * 1000 || 20000,
        open_connections_alert_threshold: openConnectionsAlertThreshold || 5,
      },
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
          options={[
            {
              value: 'slack',
              label: 'Slack',
            },
          ]}
          placeholder='Select provider'
          defaultValue={{
            value: 'slack',
            label: 'Slack',
          }}
          formatOptionLabel={ConfigLabel}
          onChange={(val, _) => val && setServiceType(val.value)}
          theme={SelectTheme}
        />
      </div>
      <div>
        <p>Authorisation Token</p>
        <TextField
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          placeholder='Auth Token'
          value={authToken}
          onChange={(e) => setAuthToken(e.target.value)}
        />
      </div>

      <div>
        <p>Channel IDs</p>
        <TextField
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          placeholder='Comma separated'
          value={channelIdString}
          onChange={(e) => setChannelIdString(e.target.value)}
        />
      </div>

      <div>
        <p>Slot Lag Alert Threshold (in GB)</p>
        <TextField
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          type={'number'}
          placeholder='optional'
          value={slotLagGBAlertThreshold}
          onChange={(e) => setSlotLagGBAlertThreshold(e.target.valueAsNumber)}
        />
      </div>

      <div>
        <p>Open Connections Alert Threshold</p>
        <TextField
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          type={'number'}
          placeholder='optional'
          value={openConnectionsAlertThreshold}
          onChange={(e) =>
            setOpenConnectionsAlertThreshold(e.target.valueAsNumber)
          }
        />
      </div>

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
};

export default NewConfig;
