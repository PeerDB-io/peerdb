'use client';

import LogsTable from '@/components/LogsTable';
import { ListMirrorNamesResponse } from '@/grpc_generated/route';
import { ProgressCircle } from '@/lib/ProgressCircle';
import ReactSelect from 'react-select';
import 'react-toastify/dist/ReactToastify.css';
import useSWR from 'swr';
import { useLocalStorage } from 'usehooks-ts';
import { fetcher } from '../utils/swr';

export default function LogsView() {
  const [mirrorName, setMirrorName] = useLocalStorage<string>(
    'peerdbMirrorNameFilterForLogs',
    ''
  );
  const [logLevel, setLogLevel] = useLocalStorage<string>(
    'peerdbLogTypeFilterForLogs',
    'all'
  );
  const { data: mirrors }: { data: ListMirrorNamesResponse; error: any } =
    useSWR('/api/v1/mirrors/names', fetcher);

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
      <LogsTable numPerPage={15} mirrorName={mirrorName} logLevel={logLevel} />
    </div>
  );
}
