'use client';
import AlertDropdown from '@/components/AlertDropdown';
import ConfigJSONView from '@/components/ConfigJSONView';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import Image from 'next/image';
import React, { useState } from 'react';
import { PulseLoader } from 'react-spinners';
import useSWR from 'swr';
import { UAlertConfigResponse } from '../dto/AlertDTO';
import { tableStyle } from '../peers/[peerName]/style';
import { fetcher } from '../utils/swr';
import { AlertConfigProps, NewConfig, ServiceType } from './new';

const ServiceIcon = ({serviceType, size}: {serviceType:string, size:number}) => {
    return <Image src={`/images/${serviceType}.png`} height={size} width={size} alt={serviceType} />;
};

const AlertConfigPage: React.FC = () => {
  const {
    data: alerts,
    isLoading,
  }: {
    data: UAlertConfigResponse[];
    error: any;
    isLoading: boolean;
  } = useSWR('/api/alert-config', fetcher);

  const blankAlert: AlertConfigProps = {
    serviceType: 'slack',
    alertConfig: {
      email_addresses: [''],
      auth_token: '',
      channel_ids: [''],
      open_connections_alert_threshold: 20,
      slot_lag_mb_alert_threshold: 5000,
    },
    forEdit: false,
  };

  const [inEditOrAddMode, setInEditOrAddMode] = useState(false);
  const [editAlertConfig, setEditAlertConfig] =
    useState<AlertConfigProps>(blankAlert);

  const onEdit = (alertConfig: UAlertConfigResponse, id: bigint) => {
    setInEditOrAddMode(true);
    const configJSON = JSON.stringify(alertConfig.service_config);

    setEditAlertConfig({
      id,
      serviceType: alertConfig.service_type as ServiceType,
      alertConfig: JSON.parse(configJSON),
      forEdit: true,
    });
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
              mirrors. Here you can configure your Slack for PeerDB to send
              alerts.
            </Label>
          </div>
          <div style={{ ...tableStyle, marginTop: '2rem', maxHeight: '25em' }}>
            <Table>
              {alerts?.length ? (
                alerts.map((alertConfig: UAlertConfigResponse, index) => (
                  <TableRow key={index}>
                    <TableCell style={{ width:20 }}>
                      <div style={{display:'flex', flexDirection:'column', alignItems:'center', justifyContent:'center'}}>
                      <div style={{display:'flex', alignItems:'center', columnGap:'0.5rem'}}>
                        <ServiceIcon serviceType={alertConfig.service_type} size={30}/>
                        <Label>{alertConfig.service_type.charAt(0).toUpperCase() + alertConfig.service_type.slice(1)}</Label>
                      </div>
                      
                      </div>
                    </TableCell>
                    <TableCell>
                      <div style={{ height: '10em' }}>
                        <ConfigJSONView
                          config={JSON.stringify(
                            alertConfig.service_config,
                            null,
                            2
                          )}
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
