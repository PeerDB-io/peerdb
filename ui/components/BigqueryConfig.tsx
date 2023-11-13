'use client';
import { blankBigquerySetting } from '@/app/peers/create/[peerType]/helpers/bq';
import { BigqueryConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import Link from 'next/link';
import { useState } from 'react';
import { PeerSetter } from './ConfigForm';
import { InfoPopover } from './InfoPopover';

interface BQProps {
  setter: PeerSetter;
}
export default function BQConfig(props: BQProps) {
  const [datasetID, setDatasetID] = useState<string>('');
  const handleJSONFile = (file: File) => {
    if (file) {
      const reader = new FileReader();
      reader.readAsText(file);
      reader.onload = () => {
        // Read the file as JSON
        let bqJson;
        try {
          bqJson = JSON.parse(reader.result as string);
        } catch (err) {
          props.setter(blankBigquerySetting);
          return;
        }
        const bqConfig: BigqueryConfig = {
          authType: bqJson.type,
          projectId: bqJson.project_id,
          privateKeyId: bqJson.private_key_id,
          privateKey: bqJson.private_key,
          clientEmail: bqJson.client_email,
          clientId: bqJson.client_id,
          authUri: bqJson.auth_uri,
          tokenUri: bqJson.token_uri,
          authProviderX509CertUrl: bqJson.auth_provider_x509_cert_url,
          clientX509CertUrl: bqJson.client_x509_cert_url,
          datasetId: datasetID,
        };
        props.setter(bqConfig);
      };
      reader.onerror = (error) => {
        console.log(error);
      };
    }
  };

  return (
    <>
      <Label>
        A service account JSON file in BigQuery is a file that contains
        information which allows PeerDB to securely access BigQuery resources.
      </Label>
      <Label
        as={Link}
        style={{ color: 'teal', textDecoration: 'underline' }}
        href='https://cloud.google.com/bigquery/docs/authentication/service-account-file'
      >
        Creating a service account file
      </Label>
      <RowWithTextField
        label={
          <Label>
            Service Account JSON
            <Tooltip
              style={{ width: '100%' }}
              content={'This is a required field.'}
            >
              <Label colorName='lowContrast' colorSet='destructive'>
                *
              </Label>
            </Tooltip>
          </Label>
        }
        action={
          <TextField
            variant='simple'
            type='file'
            style={{ border: 'none', height: 'auto' }}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              e.target.files && handleJSONFile(e.target.files[0])
            }
          />
        }
      />

      <RowWithTextField
        label={
          <Label>
            Dataset ID
            <Tooltip
              style={{ width: '100%' }}
              content={'This is a required field.'}
            >
              <Label colorName='lowContrast' colorSet='destructive'>
                *
              </Label>
            </Tooltip>
          </Label>
        }
        action={
          <div
            style={{
              display: 'flex',
              flexDirection: 'row',
              alignItems: 'center',
            }}
          >
            <TextField
              variant='simple'
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                setDatasetID(e.target.value);
                props.setter((curr) => ({
                  ...curr,
                  datasetId: e.target.value,
                }));
              }}
            />
            <InfoPopover
              tips={'ID of the dataset containing the tables you want to sync.'}
              link='https://cloud.google.com/bigquery/docs/datasets'
            />
          </div>
        }
      />
    </>
  );
}
