'use client';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { notifyErr } from '@/app/utils/notify';
import {
  CDCFlowConfigUpdate,
  FlowStatus,
  TableMapping,
} from '@/grpc_generated/flow';
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
import { ToastContainer } from 'react-toastify';
import TablePicker from '../../create/cdc/tablemapping';
import {
  changesToTablesMapping,
  reformattedTableMapping,
} from '../../create/handlers';
import { blankCDCSetting } from '../../create/helpers/common';
import { tableMappingSchema } from '../../create/schema';
import * as styles from '../../create/styles';
import { getMirrorState } from '../handlers';
import {
  notPausedCalloutStyle,
  tablesSelectedCalloutHeaderStyle,
  tablesSelectedCalloutStyle,
} from '../styles/editStyles';

type EditMirrorProps = {
  params: { mirrorId: string };
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

export default function EditMirror({ params: { mirrorId } }: EditMirrorProps) {
  const [rows, setRows] = useState<TableMapRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [mirrorState, setMirrorState] = useState<MirrorStatusResponse>();
  const [config, setConfig] = useState<CDCFlowConfigUpdate>({
    batchSize: defaultBatchSize,
    idleTimeout: defaultIdleTimeout,
    additionalTables: [],
    removedTables: [],
    numberOfSyncs: 0,
    updatedEnv: {},
    snapshotNumRowsPerPartition: defaultSnapshotNumRowsPerPartition,
    snapshotNumPartitionsOverride: defaultSnapshotNumPartitionsOverride,
    snapshotMaxParallelWorkers: defaultSnapshotMaxParallelWorkers,
    snapshotNumTablesInParallel: defaultSnapshotNumTablesInParallel,
    skipInitialSnapshotForTableAdditions: false,
  });
  const { push } = useRouter();

  const fetchStateAndUpdateDeps = useCallback(async () => {
    const res = await getMirrorState(mirrorId);
    setMirrorState(res);

    setConfig({
      batchSize:
        (res as MirrorStatusResponse).cdcStatus?.config?.maxBatchSize ||
        defaultBatchSize,
      idleTimeout:
        (res as MirrorStatusResponse).cdcStatus?.config?.idleTimeoutSeconds ||
        defaultIdleTimeout,
      additionalTables: [],
      removedTables: [],
      numberOfSyncs: 0,
      updatedEnv: {},
      snapshotNumRowsPerPartition:
        (res as MirrorStatusResponse).cdcStatus?.config
          ?.snapshotNumRowsPerPartition || defaultSnapshotNumRowsPerPartition,
      snapshotNumPartitionsOverride:
        (res as MirrorStatusResponse).cdcStatus?.config
          ?.snapshotNumPartitionsOverride ||
        defaultSnapshotNumPartitionsOverride,
      snapshotMaxParallelWorkers:
        (res as MirrorStatusResponse).cdcStatus?.config
          ?.snapshotMaxParallelWorkers || defaultSnapshotMaxParallelWorkers,
      snapshotNumTablesInParallel:
        (res as MirrorStatusResponse).cdcStatus?.config
          ?.snapshotNumTablesInParallel || defaultSnapshotNumTablesInParallel,
      skipInitialSnapshotForTableAdditions: false,
    });
  }, [mirrorId]);

  useEffect(() => {
    fetchStateAndUpdateDeps();
  }, [fetchStateAndUpdateDeps]);

  const alreadySelectedTablesMapping: Map<string, TableMapping[]> =
    useMemo(() => {
      const alreadySelectedTablesMap: Map<string, TableMapping[]> = new Map();
      mirrorState?.cdcStatus?.config?.tableMappings.forEach((value) => {
        const sourceSchema = value.sourceTableIdentifier.split('.').at(0)!;
        const mapVal: TableMapping[] =
          alreadySelectedTablesMap.get(sourceSchema) ?? [];
        // needs to be schema qualified
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

  if (!mirrorState) {
    return <ProgressCircle variant='determinate_progress_circle' />;
  }

  const sendFlowStateChangeRequest = async () => {
    if (rows.length > 0) {
      const tablesValidity = tableMappingSchema.safeParse(
        reformattedTableMapping(rows)
      );
      if (!tablesValidity.success) {
        notifyErr(tablesValidity.error.issues[0].message);
        return;
      }
    }
    setLoading(true);
    const req: FlowStateChangeRequest = {
      flowJobName: mirrorId,
      requestedFlowState: FlowStatus.STATUS_RUNNING,
      flowConfigUpdate: {
        cdcFlowConfigUpdate: { ...config, additionalTables, removedTables },
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
      notifyErr(`Something went wrong: ${res.statusText}`);
      setLoading(false);
    }
  };

  const isNotPaused =
    mirrorState.currentFlowState.toString() !==
    FlowStatus[FlowStatus.STATUS_PAUSED];

  return (
    <div>
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
              value={config.batchSize}
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
              value={config.idleTimeout}
            />
          </div>
        }
      />

      <RowWithTextField
        key={3}
        label={<Label>{'Snapshot Rows Per Partition'} </Label>}
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
                  snapshotNumRowsPerPartition: e.target.valueAsNumber,
                })
              }
              value={config.snapshotNumRowsPerPartition}
            />
          </div>
        }
      />

      <RowWithTextField
        key={4}
        label={<Label>{'Snapshot Max Parallel Workers'} </Label>}
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
                  snapshotMaxParallelWorkers: e.target.valueAsNumber,
                })
              }
              value={config.snapshotMaxParallelWorkers}
            />
          </div>
        }
      />

      <RowWithTextField
        key={5}
        label={<Label>{'Snapshot Tables In Parallel'} </Label>}
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
                  snapshotNumTablesInParallel: e.target.valueAsNumber,
                })
              }
              value={config.snapshotNumTablesInParallel}
            />
          </div>
        }
      />

      <Label variant='action' as='label' style={{ marginTop: '1rem' }}>
        Adding Tables
      </Label>
      {!isNotPaused && rows.some((row) => row.selected) && (
        <div style={tablesSelectedCalloutStyle}>
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

      <TablePicker
        sourcePeerName={mirrorState.cdcStatus?.config?.sourceName ?? ''}
        peerType={mirrorState.cdcStatus?.destinationType}
        rows={rows}
        setRows={setRows}
        alreadySelectedTablesMapping={alreadySelectedTablesMapping}
        initialLoadOnly={false}
      />

      {isNotPaused && (
        <div style={notPausedCalloutStyle}>
          Mirror can only be edited while paused.
        </div>
      )}

      <div style={styles.MirrorButtonContainer}>
        <Button
          style={styles.MirrorButtonStyle}
          onClick={() => {
            push(`/mirrors/${mirrorId}`);
          }}
        >
          Back
        </Button>
        <Button
          style={styles.MirrorButtonStyle}
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
      <ToastContainer />
    </div>
  );
}
