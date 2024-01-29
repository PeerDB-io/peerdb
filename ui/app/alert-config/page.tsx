'use client';
import { DropDialog } from '@/components/DropDialog';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import Editor from '@monaco-editor/react';
import { editor } from 'monaco-editor';
import Image from 'next/image';
import React, { useState } from 'react';
import { PulseLoader } from 'react-spinners';
import useSWR from 'swr';
import { UAlertConfigResponse } from '../dto/AlertDTO';
import { fetcher } from '../utils/swr';
import NewAlertConfig from './new';

const options: editor.IStandaloneEditorConstructionOptions = {
  readOnly: true,
  minimap: { enabled: false },
  fontSize: 14,
  lineNumbers: 'off',
  scrollBeyondLastLine: false,
};

export const ConfigJSONView = ({ config }: { config: string }) => {
  return <Editor options={options} value={config} language='json' />;
};

const ServiceIcon = (serviceType: string) => {
  switch (serviceType.toLowerCase()) {
    default:
      return <Image src='/images/slack.png' height={80} width={80} alt='alt' />;
  }
};
const AlertConfigPage: React.FC = () => {
  const {
    data: alerts,
    error,
    isLoading,
  }: {
    data: UAlertConfigResponse[];
    error: any;
    isLoading: boolean;
  } = useSWR('/api/alert-config', fetcher);
  const [newConfig, setNewConfig] = useState(false);

  return (
    <div style={{ padding: '2rem' }}>
      <Label variant='title3'>Alert Configurations</Label>
      {isLoading ? (
        <div style={{ marginTop: '1rem' }}>
          <Label>
            <PulseLoader size={10} />
          </Label>
        </div>
      ) : (
        <>
          <div>
            <Label>
              PeerDB has a built-in alerting feature to update you on your
              mirrors. Here you can configure your Slack for PeerDB to send
              alerts.
            </Label>
          </div>
          <div
            style={{ marginTop: '2rem', maxHeight: '25em', overflow: 'scroll' }}
          >
            <Table>
              {alerts?.length ? (
                alerts.map((alert: UAlertConfigResponse, index) => (
                  <TableRow key={index}>
                    <TableCell style={{ width: '10%' }}>{alert.id}</TableCell>
                    <TableCell style={{ width: '10%' }}>
                      {ServiceIcon(alert.service_type)}
                    </TableCell>
                    <TableCell>
                      <div style={{ height: '8em' }}>
                        <ConfigJSONView
                          config={JSON.stringify(alert.service_config, null, 2)}
                        />
                      </div>
                    </TableCell>
                    <TableCell style={{ width: '5%' }}>
                      <div
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                        }}
                      >
                        <DropDialog mode='ALERT' dropArgs={{ id: alert.id }} />
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell>
                    <Label as='label' style={{ fontSize: 14 }}>
                      No configurations added
                    </Label>
                  </TableCell>
                </TableRow>
              )}
            </Table>
          </div>
        </>
      )}
      <Button
        variant='normalSolid'
        disabled={newConfig}
        style={{ display: 'flex', alignItems: 'center', marginTop: '2rem' }}
        onClick={() => setNewConfig(true)}
      >
        <Icon name='add' />
        <Label as='label' style={{ fontSize: 14 }}>
          Add Configuration
        </Label>
      </Button>
      {newConfig && <NewAlertConfig />}
    </div>
  );
};

export default AlertConfigPage;
