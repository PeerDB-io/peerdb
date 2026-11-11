'use client';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { notifyErr } from '@/app/utils/notify';
import ThemedToastContainer from '@/components/ThemedToastContainer';
import {
  CDCFlowConfigUpdate,
  FlowStatus,
  TableMapping,
} from '@/grpc_generated/flow';
import {
  FlowStateChangeRequest,
  MirrorStatusResponse,
} from '@/grpc_generated/route';
import { useTheme } from '@/lib/AppTheme';
import { Button } from '@/lib/Button';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { TextField } from '@/lib/TextField';
import { useRouter } from 'next/navigation';
import React, { useMemo, useState, useTransition } from 'react';
import TablePicker from '../../create/cdc/tablemapping';
import { changesToTablesMapping } from '../../create/handlers';
import { blankCDCSetting } from '../../create/helpers/common';
import { tableMappingSchema } from '../../create/schema';
import {
  notPausedCalloutStyle,
  tablesSelectedCalloutHeaderStyle,
  tablesSelectedCalloutStyle,
} from '../styles/edit.styles';

type EditMirrorProps = {
  mirrorId: string;
  mirrorStatePromise: Promise<MirrorStatusResponse>;
};

const defaultBatchSize = blankCDCSetting.maxBatchSize;
const defaultIdleTimeout = blankCDCSetting.idleTimeoutSeconds;
const defaultSnapshotNumRowsPerPartition =
  blankCDCSetting.snapshotNumRowsPerPartition;
const defaultSnapshotNumPartitionsOverride =
  blankCDCSetting.snapshotNumPartitionsOverride;
const defaultSnapshotMaxParallelWorkers =
  blankCDCSetting.snapshotMaxParallelWorkers;
const defaultSnapshotNumTablesInParallel =
  blankCDCSetting.snapshotNumTablesInParallel;

function envStringFromState(res: MirrorStatusResponse): string {
  const env = res.cdcStatus?.config?.env ?? {};
  return Object.keys(env).length > 0 ? JSON.stringify(env, null, 2) : '';
}

// Parses the settings override textarea into the updated_env map.
// Returns an error message on invalid input.
export function parseEnvString(
  envString: string
): { env: Record<string, string> } | { error: string } {
  if (envString.trim() === '') {
    return { env: {} };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(envString);
  } catch (err: any) {
    return { error: `Settings override is not valid JSON: ${err.message}` };
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return { error: 'Settings override must be a JSON object' };
  }
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(parsed)) {
    if (typeof value !== 'string') {
      return {
        error: `Settings override value for "${key}" must be a string`,
      };
    }
    env[key] = value;
  }
  return { env };
}

function configFromState(res: MirrorStatusResponse): CDCFlowConfigUpdate {
  return {
    batchSize: res.cdcStatus?.config?.maxBatchSize || defaultBatchSize,
    idleTimeout:
      res.cdcStatus?.config?.idleTimeoutSeconds || defaultIdleTimeout,
    additionalTables: [],
    removedTables: [],
    updatedEnv: {},
    removedEnv: [],
    snapshotNumRowsPerPartition:
      res.cdcStatus?.config?.snapshotNumRowsPerPartition ||
      defaultSnapshotNumRowsPerPartition,
    snapshotNumPartitionsOverride:
      res.cdcStatus?.config?.snapshotNumPartitionsOverride ||
      defaultSnapshotNumPartitionsOverride,
    snapshotMaxParallelWorkers:
      res.cdcStatus?.config?.snapshotMaxParallelWorkers ||
      defaultSnapshotMaxParallelWorkers,
    snapshotNumTablesInParallel:
      res.cdcStatus?.config?.snapshotNumTablesInParallel ||
      defaultSnapshotNumTablesInParallel,
    skipInitialSnapshotForTableAdditions: false,
    queryCdc: undefined,
  };
}

export default function EditMirror({
  mirrorId,
  mirrorStatePromise,
}: EditMirrorProps) {
  const mirrorState = React.use(mirrorStatePromise);
  const theme = useTheme();
  const [rows, setRows] = useState<TableMapRow[]>([]);
  const [loading, startSubmit] = useTransition();
  const [config, setConfig] = useState<CDCFlowConfigUpdate>(() =>
    configFromState(mirrorState)
  );
  const [envString, setEnvString] = useState<string>(() =>
    envStringFromState(mirrorState)
  );
  const { push } = useRouter();

  const alreadySelectedTablesMapping: Map<string, TableMapping[]> =
    useMemo(() => {
      const alreadySelectedTablesMap: Map<string, TableMapping[]> = new Map();
      mirrorState?.cdcStatus?.config?.tableMappings.forEach((value) => {
        const sourceSchema = value.sourceTableIdentifier.split('.').at(0)!;
        const mapVal: TableMapping[] =
          alreadySelectedTablesMap.get(sourceSchema) ?? [];
        mapVal.push(value);
        alreadySelectedTablesMap.set(sourceSchema, mapVal);
      });
      return alreadySelectedTablesMap;
    }, [mirrorState]);

  const additionalTables = useMemo(() => {
    return changesToTablesMapping(rows, alreadySelectedTablesMapping, false);
  }, [rows, alreadySelectedTablesMapping]);

  const removedTables = useMemo(() => {
    return changesToTablesMapping(rows, alreadySelectedTablesMapping, true);
  }, [rows, alreadySelectedTablesMapping]);

  const sendFlowStateChangeRequest = () => {
    // validate only added tables; a pure removal empties the selected set which is valid when editing
    if (additionalTables.length > 0) {
      const tablesValidity = tableMappingSchema.safeParse(additionalTables);
      if (!tablesValidity.success) {
        notifyErr(tablesValidity.error.issues[0].message);
        return;
      }
    }
    const envResult = parseEnvString(envString);
    if ('error' in envResult) {
      notifyErr(envResult.error);
      return;
    }
    const currentEnv = mirrorState.cdcStatus?.config?.env ?? {};
    const removedEnv = Object.keys(currentEnv).filter(
      (key) => !(key in envResult.env)
    );
    const existingTableCount =
      mirrorState.cdcStatus?.config?.tableMappings.length ?? 0;
    if (
      existingTableCount + additionalTables.length - removedTables.length <=
      0
    ) {
      notifyErr(
        'Cannot remove all tables from a mirror; drop the mirror instead'
      );
      return;
    }
    startSubmit(async () => {
      const req: FlowStateChangeRequest = {
        flowJobName: mirrorId,
        requestedFlowState: FlowStatus.STATUS_RUNNING,
        flowConfigUpdate: {
          cdcFlowConfigUpdate: {
            ...config,
            additionalTables,
            removedTables,
            updatedEnv: envResult.env,
            removedEnv,
          },
        },
        dropMirrorStats: false,
        skipDestinationDrop: false,
      };
      const res = await fetch('/api/v1/mirrors/state_change', {
        method: 'POST',
        body: JSON.stringify(req),
        cache: 'no-store',
      });
      if (res.ok) {
        push(`/mirrors/${mirrorId}`);
      } else {
        const body = await res.json().catch(() => null);
        notifyErr(body?.message || `Something went wrong: ${res.statusText}`);
      }
    });
  };

  const isNotPaused =
    mirrorState.currentFlowState.toString() !==
    FlowStatus[FlowStatus.STATUS_PAUSED];

  const settingLabel: React.CSSProperties = {
    display: 'flex',
    flexDirection: 'column',
    gap: 6,
    minWidth: 0,
    fontSize: 13,
    fontWeight: 500,
  };

  return (
    <div>
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(5, minmax(0, 1fr)) minmax(0, 1.4fr)',
          gap: 12,
          alignItems: 'end',
        }}
      >
        <label style={settingLabel}>
          Pull Batch Size
          <TextField
            variant='simple'
            type={'number'}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setConfig({
                ...config,
                batchSize: e.target.valueAsNumber,
              })
            }
            value={Number.isNaN(config.batchSize) ? '' : config.batchSize}
          />
        </label>
        <label style={settingLabel}>
          Sync Interval (Seconds)
          <TextField
            variant='simple'
            type={'number'}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setConfig({
                ...config,
                idleTimeout: e.target.valueAsNumber,
              })
            }
            value={Number.isNaN(config.idleTimeout) ? '' : config.idleTimeout}
          />
        </label>
        <label style={settingLabel}>
          Snapshot Rows Per Partition
          <TextField
            variant='simple'
            type={'number'}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setConfig({
                ...config,
                snapshotNumRowsPerPartition: e.target.valueAsNumber,
              })
            }
            value={
              Number.isNaN(config.snapshotNumRowsPerPartition)
                ? ''
                : config.snapshotNumRowsPerPartition
            }
          />
        </label>
        <label style={settingLabel}>
          Snapshot Max Parallel Workers
          <TextField
            variant='simple'
            type={'number'}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setConfig({
                ...config,
                snapshotMaxParallelWorkers: e.target.valueAsNumber,
              })
            }
            value={
              Number.isNaN(config.snapshotMaxParallelWorkers)
                ? ''
                : config.snapshotMaxParallelWorkers
            }
          />
        </label>
        <label style={settingLabel}>
          Snapshot Tables In Parallel
          <TextField
            variant='simple'
            type={'number'}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setConfig({
                ...config,
                snapshotNumTablesInParallel: e.target.valueAsNumber,
              })
            }
            value={
              Number.isNaN(config.snapshotNumTablesInParallel)
                ? ''
                : config.snapshotNumTablesInParallel
            }
          />
        </label>
        <label style={settingLabel}>
          Settings override
          <TextField
            variant='simple'
            value={envString}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setEnvString(e.target.value)
            }
          />
        </label>
      </div>

      {!isNotPaused && additionalTables.length > 0 && (
        <div style={tablesSelectedCalloutStyle(theme.theme)}>
          <div style={tablesSelectedCalloutHeaderStyle}>
            Note on adding tables
          </div>
          CDC will be put on hold until initial load for these added tables have
          been completed.
          <br />
          The <b>replication slot will grow</b> during this period.
          <br />
          For custom publications, ensure that the tables are part of the
          publication you provided. This can be done with{' '}
          <code>ALTER PUBLICATION pubname ADD TABLE table1, table2;</code>
        </div>
      )}
      {!isNotPaused && removedTables.length > 0 && (
        <div style={tablesSelectedCalloutStyle(theme.theme)}>
          <div style={tablesSelectedCalloutHeaderStyle}>
            Note on removing tables
          </div>
          Deselect a currently mirrored table to remove it from this mirror.
          <br />
          For custom publications, also drop the table from the publication
          yourself with{' '}
          <code>ALTER PUBLICATION pubname DROP TABLE table1, table2;</code>
        </div>
      )}

      <TablePicker
        redesign
        sourcePeerName={mirrorState.cdcStatus?.config?.sourceName ?? ''}
        sourceType={mirrorState.cdcStatus?.sourceType}
        peerType={mirrorState.cdcStatus?.destinationType}
        rows={rows}
        setRows={setRows}
        alreadySelectedTablesMapping={alreadySelectedTablesMapping}
        initialLoadOnly={false}
      />

      {isNotPaused && (
        <div style={notPausedCalloutStyle(theme.theme)}>
          Mirror can only be edited while paused.
        </div>
      )}

      <div
        style={{
          position: 'fixed',
          right: '1.5rem',
          bottom: '1.5rem',
          zIndex: 20,
          display: 'flex',
          gap: '0.75rem',
        }}
      >
        <Button
          style={{ height: '3.25rem', minWidth: '8rem', borderRadius: '2rem', fontWeight: 600, fontSize: 16 }}
          onClick={() => {
            push(`/mirrors/${mirrorId}`);
          }}
        >
          Back
        </Button>
        <Button
          style={{ height: '3.25rem', minWidth: '10rem', borderRadius: '2rem', fontWeight: 600, fontSize: 16 }}
          variant='normalSolid'
          disabled={loading || isNotPaused}
          onClick={sendFlowStateChangeRequest}
        >
          {loading ? (
            <ProgressCircle variant='determinate_progress_circle' />
          ) : (
            'Edit Mirror'
          )}
        </Button>
      </div>
      <ThemedToastContainer />
    </div>
  );
}
