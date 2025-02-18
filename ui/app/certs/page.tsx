'use client';
import { notifyErr } from '@/app/utils/notify';
import { GetCertsResponse } from '@/grpc_generated/route';
import { Label } from '@/lib/Label/Label';
import { TextField } from '@/lib/TextField';
import { useState } from 'react';
import useSWR from 'swr';
import { fetcher } from '../utils/swr';
import { HandleAddCert } from './handlers';
import CertsTable from './list';

export default function CertsPage() {
  const { data: res, isLoading } = useSWR<GetCertsResponse>(
    '/api/v1/certs/-1',
    fetcher
  );
  const [newName, setNewName] = useState('');
  const [newFile, setNewFile] = useState('');

  const handleFile = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files && e.target.files[0];
    if (file) {
      const reader = new FileReader();
      reader.readAsText(file);
      reader.onload = () => {
        setNewFile(reader.result as string);
      };
      reader.onerror = (error) => {
        console.log(error);
      };
    }
  };

  const handleAdd = () => {
    if (!newFile) {
      notifyErr('Empty certificates not allowed');
      return;
    }
    if (!newName) {
      notifyErr('Please enter a certificate name');
      return;
    }
    HandleAddCert({ id: -1, name: newName, source: newFile }).then(
      (success) => {
        if (success) {
          notifyErr('Certificate uploaded', true);
        } else {
          notifyErr('Certificate upload failed');
        }
      }
    );
  };

  return (
    <div
      style={{
        padding: '1rem',
        display: 'flex',
        flexDirection: 'column',
        width: '100%',
        rowGap: '1rem',
      }}
    >
      <div>
        <div
          style={{
            display: 'flex',
            width: '100%',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}
        >
          <Label variant='title3'>Certs</Label>
          <TextField
            variant='simple'
            placeholder='Name'
            style={{ border: 'auto' }}
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
          />
          <TextField
            variant='simple'
            type='file'
            style={{ border: 'auto' }}
            onChange={handleFile}
          />
          <input type='button' value='Upload' onClick={() => handleAdd()} />
        </div>
      </div>
      <div>
        <Label>This is allows you to add additional root CA certs</Label>
      </div>
      {!isLoading && res && <CertsTable certs={res.certs} />}
    </div>
  );
}
