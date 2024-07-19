'use client';

import LogsTable from '@/components/LogsTable';
import {
  ListMirrorLogsRequest,
  ListMirrorLogsResponse,
  ListMirrorNamesResponse,
  MirrorLog,
} from '@/grpc_generated/route';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { useEffect, useState } from 'react';
import ReactSelect from 'react-select';
import 'react-toastify/dist/ReactToastify.css';
import useSWR from 'swr';
import { useLocalStorage } from 'usehooks-ts';
import { fetcher } from '../utils/swr';

export default function LogsView() {
  const [logs, setLogs] = useState<MirrorLog[]>([]);
  const [mirrorName, setMirrorName] = useLocalStorage<string>(
    'peerdbMirrorNameFilterForLogs',
    ''
  );
  const [logLevel, setLogLevel] = useLocalStorage<string>(
    'peerdbLogTypeFilterForLogs',
    'all'
  );
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const { data: mirrors }: { data: ListMirrorNamesResponse; error: any } =
    useSWR('/api/v1/mirrors/names', fetcher);

  useEffect(() => {
    setCurrentPage(1);
  }, [mirrorName]);

  useEffect(() => {
    const req: ListMirrorLogsRequest = {
      level: logLevel,
      flowJobName: mirrorName,
      page: currentPage,
      numPerPage: 15,
    };

    const fetchData = async () => {
      try {
        const response = await fetch('/api/v1/mirrors/logs', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          cache: 'no-store',
          body: JSON.stringify(req),
        });
        const data: ListMirrorLogsResponse = await response.json();
        const numPages = Math.ceil(data.total / req.numPerPage);
        setLogs(data.errors);
        setTotalPages(numPages);
      } catch (error) {
        console.error('Error fetching mirror logs:', error);
      }
    };

    fetchData();
  }, [currentPage, mirrorName, logLevel]);

  if (!mirrors) {
    return <ProgressCircle variant='determinate_progress_circle' />;
  }
  return (
    <div style={{ width: '100%', padding: '2rem' }}>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          columnGap: '2rem',
          marginBottom: '1rem',
        }}
      >
        <div style={{ width: 'fit-content' }}>
          <ReactSelect
            isClearable={true}
            defaultValue={
              mirrorName ? { value: mirrorName, label: mirrorName } : undefined
            }
            options={mirrors.names.map((mirror) => ({
              value: mirror,
              label: mirror,
            }))}
            onChange={(selectedOption) =>
              setMirrorName(selectedOption?.value ?? '')
            }
            placeholder='Filter by mirror'
          />
        </div>
        <div style={{ width: 'fit-content' }}>
          <ReactSelect
            defaultValue={{ value: logLevel, label: logLevel }}
            options={['all', 'error', 'warning', 'info'].map((type) => ({
              value: type,
              label: type,
            }))}
            onChange={(selectedOption) =>
              setLogLevel(selectedOption?.value ?? 'all')
            }
            placeholder='Filter by log type'
          />
        </div>
      </div>
      <LogsTable
        logs={logs}
        currentPage={currentPage}
        totalPages={totalPages}
        setCurrentPage={setCurrentPage}
      />
    </div>
  );
}
