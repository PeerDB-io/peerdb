'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { blankEventhubSetting } from '@/app/peers/create/[peerType]/helpers/eh';
import { EventHubConfig, EventHubGroupConfig } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { useEffect, useState } from 'react';
import { ToastContainer } from 'react-toastify';
import EventhubsConfig from './EventhubConfig';
interface EventhubsProps {
  groupConfig: EventHubGroupConfig;
  setter: PeerSetter;
}

export default function EventhubsForm({ groupConfig, setter }: EventhubsProps) {
  const [currConfigForms, setCurrConfigForms] = useState<
    (EventHubConfig & { id: number })[]
  >([]);

  const handleAddNamespace = () => {
    setCurrConfigForms([
      ...currConfigForms,
      { ...blankEventhubSetting, id: currConfigForms.length },
    ]);
  };
  const handleDeleteNamespace = (id: number) => {
    setCurrConfigForms((prev) => prev.filter((_, index) => index !== id));
  };

  useEffect(() => {
    // construct a map of namespace to eventhub config
    const eventhubs = currConfigForms.reduce((acc, curr) => {
      return {
        ...acc,
        [curr.namespace]: curr,
      };
    }, {});
    setter((prev) => {
      return {
        ...prev,
        eventhubs,
      };
    });
  }, [currConfigForms, setter]);

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        rowGap: '1rem',
        marginBottom: '1rem',
      }}
    >
      <Label>
        The Eventhubs peer requires namespaces to be specified. <br></br>
        Below you can add namespaces and their details. Save to persist the
        changes.
      </Label>
      {currConfigForms.map((ehConfig) => {
        return (
          <EventhubsConfig
            key={ehConfig.id}
            eventhubConfig={ehConfig}
            updateForms={setCurrConfigForms}
            onDelete={() => handleDeleteNamespace(ehConfig.id)}
          />
        );
      })}
      <Button
        variant='normalSolid'
        onClick={handleAddNamespace}
        style={{ width: 'fit-content' }}
      >
        <Icon name='add' /> Namespace
      </Button>
      <ToastContainer containerId={'for eventhubs'} />
    </div>
  );
}
