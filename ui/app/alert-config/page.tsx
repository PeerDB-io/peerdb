'use client';
import AlertDropdown from '@/components/AlertDropdown';
import ConfigJSONView from '@/components/ConfigJSONView';
import { AlertConfig, GetAlertConfigsResponse } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import Image from 'next/image';
import React, { useState } from 'react';
import { PulseLoader } from 'react-spinners';
import useSWR from 'swr';
import { tableStyle } from '../peers/[peerName]/style';
import { fetcher } from '../utils/swr';
import { AlertConfigProps, NewConfig, ServiceType } from './new';

const ServiceIcon = ({
  serviceType,
  size,
}: {
  serviceType: string;
  size: number;
}) => (
  <Image
    src={`/images/${serviceType}.png`}
    height={size}
    width={size}
    alt={serviceType}
  />
);

const AlertConfigPage: React.FC = () => {
  const {
    data: alerts,
    isLoading,
  }: {
    data: GetAlertConfigsResponse;
    error: any;
    isLoading: boolean;
  } = useSWR('/api/v1/alerts/config', fetcher);

  const blankAlert: AlertConfigProps = {
    serviceType: 'slack',
    alertConfig: {
      email_addresses: [''],
      auth_token: '',
      channel_ids: [''],
      open_connections_alert_threshold: 20,
      slot_lag_mb_alert_threshold: 5000,
    },
    alertForMirrors: [],
    forEdit: false,
  };

  const [inEditOrAddMode, setInEditOrAddMode] = useState(false);
  const [editAlertConfig, setEditAlertConfig] =
    useState<AlertConfigProps>(blankAlert);

  const onEdit = (alertConfig: AlertConfig, id: number) => {
    setInEditOrAddMode(true);

    setEditAlertConfig({
      id,
      serviceType: alertConfig.serviceType as ServiceType,
      alertConfig: JSON.parse(alertConfig.serviceConfig),
      forEdit: true,
      alertForMirrors: alertConfig.alertForMirrors,
    });
  };

  const genConfigJSON = (alertConfig: AlertConfig) => {
    const parsedConfig = JSON.parse(alertConfig.serviceConfig);
    return JSON.stringify(
      { ...parsedConfig, alertForMirrors: alertConfig.alertForMirrors },
      null,
      2
    );
  };

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
              mirrors. Here you can configure your Alert Provider for PeerDB to
              send alerts.
            </Label>
          </div>
          <div style={{ ...tableStyle, marginTop: '2rem', maxHeight: '25em' }}>
            <Table>
              {alerts?.configs?.length ? (
                alerts.configs.map((alertConfig: AlertConfig, index) => (
                  <TableRow key={index}>
                    <TableCell style={{ width: 20 }}>
                      <div
                        style={{
                          display: 'flex',
                          flexDirection: 'column',
                          alignItems: 'center',
                          justifyContent: 'center',
                        }}
                      >
                        <div
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            columnGap: '0.5rem',
                          }}
                        >
                          <ServiceIcon
                            serviceType={alertConfig.serviceType}
                            size={30}
                          />
                          <Label>
                            {alertConfig.serviceType.charAt(0).toUpperCase() +
                              alertConfig.serviceType.slice(1)}
                          </Label>
                        </div>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div style={{ height: '10em' }}>
                        <ConfigJSONView config={genConfigJSON(alertConfig)} />
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
                        <AlertDropdown
                          disable={inEditOrAddMode}
                          alertId={alertConfig.id}
                          onEdit={() => onEdit(alertConfig, alertConfig.id)}
                        />
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
        variant={inEditOrAddMode ? 'peer' : 'normalSolid'}
        style={{ display: 'flex', alignItems: 'center', marginTop: '2rem' }}
        onClick={() => {
          if (inEditOrAddMode) {
            setEditAlertConfig(blankAlert);
          }
          setInEditOrAddMode((prev) => !prev);
        }}
      >
        <Icon name={inEditOrAddMode ? 'cancel' : 'add'} />
        <Label as='label' style={{ fontSize: 14 }}>
          {inEditOrAddMode ? 'Cancel' : 'Add Configuration'}
        </Label>
      </Button>
      {inEditOrAddMode && <NewConfig {...editAlertConfig} />}
    </div>
  );
};

export default AlertConfigPage;
