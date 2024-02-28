import { Button } from '@/lib/Button';
import { TextField } from '@/lib/TextField';
import Image from 'next/image';
import { useState } from 'react';
import ReactSelect from 'react-select';
import { PulseLoader } from 'react-spinners';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { alertConfigReqSchema, alertConfigType } from './validation';

export interface AlertConfigProps {
  id?: bigint;
  serviceType: string;
  authToken: string;
  channelIdString: string;
  slotLagMBAlertThreshold: number;
  openConnectionsAlertThreshold: number;
  forEdit?: boolean;
}

const notifyErr = (errMsg: string) => {
  toast.error(errMsg, {
    position: 'bottom-center',
  });
};

function ConfigLabel() {
  return (
    <div style={{ display: 'flex', alignItems: 'center' }}>
      <Image src={'/images/slack.png'} alt='slack' height={60} width={60} />
    </div>
  );
}

const NewAlertConfig = (alertProps: AlertConfigProps) => {
  const [serviceType, setServiceType] = useState<string>('slack');
  const [authToken, setAuthToken] = useState<string>(alertProps.authToken);
  const [channelIdString, setChannelIdString] = useState<string>(
    alertProps.channelIdString
  );
  const [slotLagMBAlertThreshold, setSlotLagMBAlertThreshold] =
    useState<number>(alertProps.slotLagMBAlertThreshold);
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
        slot_lag_mb_alert_threshold: slotLagMBAlertThreshold || 5000,
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
        <p>Slot Lag Alert Threshold (in MB)</p>
        <TextField
          style={{ height: '2.5rem', marginTop: '0.5rem' }}
          variant='simple'
          type={'number'}
          placeholder='optional'
          value={slotLagMBAlertThreshold}
          onChange={(e) => setSlotLagMBAlertThreshold(e.target.valueAsNumber)}
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

export default NewAlertConfig;
