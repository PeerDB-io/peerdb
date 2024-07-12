'use client';

import LogsTable from '@/components/LogsTable';
import {
  ListMirrorLogsRequest,
  ListMirrorLogsResponse,
  MirrorLog,
} from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { useParams } from 'next/navigation';
import { useEffect, useState } from 'react';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

export default function MirrorError() {
  const params = useParams<{ mirrorName: string }>();
  const [mirrorErrors, setMirrorErrors] = useState<MirrorLog[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);

  useEffect(() => {
    setCurrentPage(1);
  }, [params.mirrorName]);

  useEffect(() => {
    const req: ListMirrorLogsRequest = {
      flowJobName: params.mirrorName,
      page: currentPage,
      numPerPage: 10,
      level: 'all',
    };

    const fetchData = async () => {
      try {
        const response = await fetch('/api/mirrors/errors', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          cache: 'no-store',
          body: JSON.stringify(req),
        });
        const data: ListMirrorLogsResponse = await response.json();
        const numPages = Math.ceil(data.total / req.numPerPage);
        setMirrorErrors(data.errors);
        setTotalPages(numPages);
      } catch (error) {
        console.error('Error fetching mirror errors:', error);
      }
    };

    fetchData();
  }, [currentPage, params.mirrorName]);

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
            logs={mirrorErrors}
            currentPage={currentPage}
            totalPages={totalPages}
            setCurrentPage={setCurrentPage}
          />
        </div>
      </div>
      <ToastContainer />
    </>
  );
}
