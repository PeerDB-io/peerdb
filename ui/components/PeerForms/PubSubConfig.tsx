'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { blankPubSubSetting } from '@/app/peers/create/[peerType]/helpers/ps';
import { PubSubConfig } from '@/grpc_generated/peers';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { TextField } from '@/lib/TextField';
import { Tooltip } from '@/lib/Tooltip';
import Link from 'next/link';
import { useState } from 'react';

interface PSProps {
  setter: PeerSetter;
}
export default function PubSubForm(props: PSProps) {
  const [datasetID, setDatasetID] = useState<string>('');
  const handleJSONFile = (file: File) => {
    if (file) {
      const reader = new FileReader();
      reader.readAsText(file);
      reader.onload = () => {
        // Read the file as JSON
        let psJson;
        try {
          psJson = JSON.parse(reader.result as string);
        } catch (err) {
          props.setter(blankPubSubSetting);
          return;
        }
        const psConfig: PubSubConfig = {
          serviceAccount: {
            authType: psJson.type ?? psJson.auth_type,
            projectId: psJson.project_id,
            privateKeyId: psJson.private_key_id,
            privateKey: psJson.private_key,
            clientEmail: psJson.client_email,
            clientId: psJson.client_id,
            authUri: psJson.auth_uri,
            tokenUri: psJson.token_uri,
            authProviderX509CertUrl: psJson.auth_provider_x509_cert_url,
            clientX509CertUrl: psJson.client_x509_cert_url,
          },
        };
        props.setter(psConfig);
      };
      reader.onerror = (error) => {
        console.log(error);
      };
    }
  };

  return (
    <>
      <Label>
        A service account JSON file allows PeerDB to securely access PubSub
        resources.
      </Label>
      <Label
        as={Link}
        style={{ color: 'teal', textDecoration: 'underline', display: 'block' }}
        href='https://cloud.google.com/bigquery/docs/authentication/service-account-file'
        target='_blank'
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
    </>
  );
}
