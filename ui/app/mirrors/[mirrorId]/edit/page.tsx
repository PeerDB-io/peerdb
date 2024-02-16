'use client';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { CDCFlowConfigUpdate, FlowStatus } from '@/grpc_generated/flow';
import {
  FlowStateChangeRequest,
  MirrorStatusResponse,
} from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { TextField } from '@/lib/TextField';
import { useRouter } from 'next/navigation';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import TableMapping from '../../create/cdc/tablemapping';
import { reformattedTableMapping } from '../../create/handlers';
import { blankCDCSetting } from '../../create/helpers/common';
type EditMirrorProps = {
  params: { mirrorId: string };
};

const notifyErr = (errMsg: string) => {
  toast.error(errMsg, {
    position: 'bottom-center',
  });
};

const EditMirror = ({ params: { mirrorId } }: EditMirrorProps) => {
  const defaultBatchSize = blankCDCSetting.maxBatchSize;
  const defaultIdleTimeout = blankCDCSetting.idleTimeoutSeconds;

  const [rows, setRows] = useState<TableMapRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [mirrorState, setMirrorState] = useState<MirrorStatusResponse>();
  const [config, setConfig] = useState<CDCFlowConfigUpdate>({
    batchSize: defaultBatchSize,
    idleTimeout: defaultIdleTimeout,
    additionalTables: [],
  });
  const { push } = useRouter();

  const fetchStateAndUpdateDeps = useCallback(async () => {
    await fetch('/api/mirrors/state', {
      method: 'POST',
      body: JSON.stringify({
        flowJobName: mirrorId,
      }),
    })
      .then((res) => res.json())
      .then((res) => {
        setMirrorState(res);

        setConfig({
          batchSize:
            (res as MirrorStatusResponse).cdcStatus?.config?.maxBatchSize ||
            defaultBatchSize,
          idleTimeout:
            (res as MirrorStatusResponse).cdcStatus?.config
              ?.idleTimeoutSeconds || defaultIdleTimeout,
          additionalTables: [],
        });
      });
  }, [mirrorId, defaultBatchSize, defaultIdleTimeout]);

  useEffect(() => {
    fetchStateAndUpdateDeps();
  }, [fetchStateAndUpdateDeps]);

  const omitAdditionalTablesMapping: Map<string, string[]> = useMemo(() => {
    const omitAdditionalTablesMapping: Map<string, string[]> = new Map();
    mirrorState?.cdcStatus?.config?.tableMappings.forEach((value) => {
      const sourceSchema = value.sourceTableIdentifier.split('.').at(0)!;
      const mapVal: string[] =
        omitAdditionalTablesMapping.get(sourceSchema) || [];
      // needs to be schema qualified
      mapVal.push(value.sourceTableIdentifier);
      omitAdditionalTablesMapping.set(sourceSchema, mapVal);
    });
    return omitAdditionalTablesMapping;
  }, [mirrorState]);

  const additionalTables = useMemo(() => {
    return reformattedTableMapping(rows);
  }, [rows]);

  if (!mirrorState) {
    return <ProgressCircle variant='determinate_progress_circle' />;
  }

  const sendFlowStateChangeRequest = async () => {
    setLoading(true);
    const req: FlowStateChangeRequest = {
      flowJobName: mirrorId,
      sourcePeer: mirrorState.cdcStatus?.config?.source,
      destinationPeer: mirrorState.cdcStatus?.config?.destination,
      requestedFlowState: FlowStatus.STATUS_UNKNOWN,
      flowConfigUpdate: {
        cdcFlowConfigUpdate: { ...config, additionalTables },
      },
    };
    const res = await fetch(`/api/mirrors/state_change`, {
      method: 'POST',
      body: JSON.stringify(req),
      cache: 'no-store',
    });
    if (res.ok) {
      push(`/mirrors/${mirrorId}`);
    } else {
      notifyErr(`Something went wrong: ${res.statusText}`);
      setLoading(false);
    }
  };

  return (
    <div>
      <Label variant='title3'>Edit {mirrorId}</Label>

      <RowWithTextField
        key={1}
        label={<Label>{'Pull Batch Size'} </Label>}
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
              type={'number'}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setConfig({
                  ...config,
                  batchSize: e.target.valueAsNumber,
                })
              }
              defaultValue={config.batchSize}
            />
          </div>
        }
      />

      <RowWithTextField
        key={2}
        label={<Label>{'Sync Interval (Seconds)'} </Label>}
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
              type={'number'}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setConfig({
                  ...config,
                  idleTimeout: e.target.valueAsNumber,
                })
              }
              defaultValue={config.idleTimeout}
            />
          </div>
        }
      />

      <TableMapping
        sourcePeerName={mirrorState.cdcStatus?.config?.source?.name || ''}
        peerType={mirrorState.cdcStatus?.config?.destination?.type}
        rows={rows}
        setRows={setRows}
        omitAdditionalTablesMapping={omitAdditionalTablesMapping}
      />

      <div style={{ display: 'flex' }}>
        <Button
          style={{
            marginTop: '1rem',
            marginRight: '1rem',
            width: '8%',
            height: '2.5rem',
          }}
          variant='normalSolid'
          disabled={
            loading ||
            (additionalTables.length > 0 &&
              mirrorState.currentFlowState.toString() !==
                FlowStatus[FlowStatus.STATUS_PAUSED])
          }
          onClick={sendFlowStateChangeRequest}
        >
          {loading ? (
            <ProgressCircle variant='determinate_progress_circle' />
          ) : (
            'Edit Mirror'
          )}
        </Button>
        <Button
          style={{ marginTop: '1rem', width: '8%', height: '2.5rem' }}
          onClick={() => {
            push(`/mirrors/${mirrorId}`);
          }}
        >
          Back
        </Button>
      </div>
      <ToastContainer />
    </div>
  );
};

export default EditMirror;
