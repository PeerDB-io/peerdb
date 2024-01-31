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
import { TextField } from '@/lib/TextField';
import { ProgressCircle } from '@tremor/react';
import { useRouter } from 'next/navigation';
import { useCallback, useEffect, useMemo, useState } from 'react';
import TableMapping from '../../create/cdc/tablemapping';
import { reformattedTableMapping } from '../../create/handlers';

type EditMirrorProps = {
  params: { mirrorId: string };
};

const EditMirror = ({ params: { mirrorId } }: EditMirrorProps) => {
  const [rows, setRows] = useState<TableMapRow[]>([]);
  const [mirrorState, setMirrorState] = useState<MirrorStatusResponse>();
  const [config, setConfig] = useState<CDCFlowConfigUpdate>({
    batchSize: 1000000,
    idleTimeout: 60,
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
            1000000,
          idleTimeout:
            (res as MirrorStatusResponse).cdcStatus?.config
              ?.idleTimeoutSeconds || 60,
          additionalTables: [],
        });
      });
  }, [mirrorId]);

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
  useMemo(() => {
    setConfig((c) => ({
      ...c,
      additionalTables: reformattedTableMapping(rows),
    }));
  }, [rows]);

  if (!mirrorState) {
    return <ProgressCircle />;
  }

  // todo: use mirrorId (which is mirrorName) to query flows table/temporal and get config
  // you will have to decode the config to get the table mapping. see: /mirrors/page.tsx
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
      <Button
        style={{ marginTop: '1rem', width: '8%', height: '2.5rem' }}
        variant='normalSolid'
        disabled={
          config.additionalTables.length > 0 &&
          mirrorState.currentFlowState.toString() !==
            FlowStatus[FlowStatus.STATUS_PAUSED]
        }
        onClick={async () => {
          const req: FlowStateChangeRequest = {
            flowJobName: mirrorId,
            sourcePeer: mirrorState.cdcStatus?.config?.source,
            destinationPeer: mirrorState.cdcStatus?.config?.destination,
            requestedFlowState: FlowStatus.STATUS_UNKNOWN,
            flowConfigUpdate: {
              cdcFlowConfigUpdate: config,
            },
          };
          await fetch(`/api/mirrors/state_change`, {
            method: 'POST',
            body: JSON.stringify(req),
            cache: 'no-store',
          });
          push(`/mirrors/${mirrorId}`);
        }}
      >
        Edit Mirror
      </Button>
    </div>
  );
};

export default EditMirror;
