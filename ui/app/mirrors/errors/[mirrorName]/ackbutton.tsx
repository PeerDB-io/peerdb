'use client';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { useState } from 'react';
import { toast } from 'react-toastify';

const notifyErr = (errMsg: string) => {
  toast.error(errMsg, {
    position: 'bottom-center',
  });
};

const AckButton = ({ ack, id }: { ack: boolean; id: number | bigint }) => {
  const [loading, setLoading] = useState(false);
  const [updated, setUpdated] = useState(false);
  // handleAck updates ack to true for the given mirrorID
  const handleAck = async (mirrorID: bigint | number) => {
    setLoading(true);
    const updateResResult = await fetch('/api/mirrors/alerts', {
      method: 'PUT',
      body: JSON.stringify({
        mirrorIDStringList: [mirrorID.toString()],
      }),
    });
    const updateRes = await updateResResult.json();
    setLoading(false);
    if (!updateRes) {
      notifyErr('Something went wrong when trying to acknowledge');
      return;
    }
    setUpdated(true);
  };
  return (
    <>
      {ack !== true && updated !== true ? (
        <Button variant='normalSolid' onClick={() => handleAck(id)}>
          <Label as='label' style={{ fontSize: 13 }}>
            {loading ? (
              <ProgressCircle variant='intermediate_progress_circle' />
            ) : (
              'Acknowledge'
            )}
          </Label>
        </Button>
      ) : (
        <Button variant='normal' disabled={true}>
          <Label as='label' style={{ fontSize: 13 }}>
            Acknowledged
          </Label>
        </Button>
      )}
    </>
  );
};

export default AckButton;
