import { Button } from '@/lib/Button';
import { TextField } from '@/lib/TextField';
import Image from 'next/image';
import { useState } from 'react';
import ReactSelect from 'react-select';
import { PulseLoader } from 'react-spinners';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { alertConfigReqSchema, alertConfigType } from './validation';
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

const NewAlertConfig = () => {
  const [serviceType, setServiceType] = useState<string>();
  const [authToken, setAuthToken] = useState<string>();
  const [channelIdString, setChannelIdString] = useState<string>();
  const [slotLagMBAlertThreshold, setSlotLagMBAlertThreshold] =
    useState<number>();
  const [openConnectionsAlertThreshold, setOpenConnectionsAlertThreshold] =
    useState<number>();
  const [loading, setLoading] = useState(false);
  const handleAdd = async () => {
    if (serviceType !== 'slack') {
      notifyErr('Service Type must be selected');
      return;
    }
    console.log(slotLagMBAlertThreshold);
    console.log(openConnectionsAlertThreshold);
    const alertConfigReq: alertConfigType = {
      serviceType: serviceType,
      serviceConfig: {
        auth_token: authToken ?? '',
        channel_ids: channelIdString?.split(',')!,
        slot_lag_mb_alert_threshold: slotLagMBAlertThreshold || 0,
        open_connections_alert_threshold: openConnectionsAlertThreshold || 0,
      },
    };
    const alertReqValidity = alertConfigReqSchema.safeParse(alertConfigReq);
    if (!alertReqValidity.success) {
      notifyErr(alertReqValidity.error.issues[0].message);
      return;
    }
    setLoading(true);
    const createRes = await fetch('/api/alert-config', {
      method: 'POST',
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
        {loading ? <PulseLoader color='white' size={10} /> : 'Create'}
      </Button>
      <ToastContainer />
    </div>
  );
};

export default NewAlertConfig;
