'use client';

import LogsTable from '@/components/LogsTable';
import { Label } from '@/lib/Label';
import { useParams } from 'next/navigation';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

export default function MirrorError() {
  const params = useParams<{ mirrorName: string }>();

  return (
    <>
      <div style={{ padding: '2rem' }}>
        <Label variant='title2'>Logs</Label>
        <hr></hr>
        <div style={{ marginTop: '1rem' }}>
          <Label variant='body'>
            <b>Mirror name</b>:
          </Label>
          <Label variant='body'>
            <a href={`/mirrors/${params.mirrorName}`}>{params.mirrorName}</a>
          </Label>

          <div>
            <Label as='label' style={{ fontSize: 14, marginTop: '1rem' }}>
              Here you can view logs for your mirror.
            </Label>
          </div>

          <LogsTable
            numPerPage={10}
            logLevel='all'
            mirrorName={params.mirrorName}
          />
        </div>
      </div>
      <ToastContainer />
    </>
  );
}
