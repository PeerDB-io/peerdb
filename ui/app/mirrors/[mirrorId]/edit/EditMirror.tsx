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
import { Label } from '@/lib/Label';
import { RowWithTextField } from '@/lib/Layout';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { TextField } from '@/lib/TextField';
import { useRouter } from 'next/navigation';
import React, { useMemo, useState, useTransition } from 'react';
import TablePicker from '../../create/cdc/tablemapping';
import {
  changesToTablesMapping,
  reformattedTableMapping,
} from '../../create/handlers';
import { blankCDCSetting } from '../../create/helpers/common';
import { tableMappingSchema } from '../../create/schema';
import * as styles from '../../create/styles';
import {
  fieldStyle,
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

function configFromState(res: MirrorStatusResponse): CDCFlowConfigUpdate {
  return {
    batchSize: res.cdcStatus?.config?.maxBatchSize || defaultBatchSize,
    idleTimeout:
      res.cdcStatus?.config?.idleTimeoutSeconds || defaultIdleTimeout,
    additionalTables: [],
    removedTables: [],
    numberOfSyncs: 0,
    updatedEnv: {},
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
    if (rows.length > 0) {
      const tablesValidity = tableMappingSchema.safeParse(
        reformattedTableMapping(rows)
      );
      if (!tablesValidity.success) {
        notifyErr(tablesValidity.error.issues[0].message);
        return;
      }
    }
    startSubmit(async () => {
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
      }
    });
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
          <div style={fieldStyle}>
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
          </div>
        }
      />

      <RowWithTextField
        key={2}
        label={<Label>{'Sync Interval (Seconds)'} </Label>}
        action={
          <div style={fieldStyle}>
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
          </div>
        }
      />

      <RowWithTextField
        key={3}
        label={<Label>{'Snapshot Rows Per Partition'} </Label>}
        action={
          <div style={fieldStyle}>
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
          </div>
        }
      />

      <RowWithTextField
        key={4}
        label={<Label>{'Snapshot Max Parallel Workers'} </Label>}
        action={
          <div style={fieldStyle}>
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
          </div>
        }
      />

      <RowWithTextField
        key={5}
        label={<Label>{'Snapshot Tables In Parallel'} </Label>}
        action={
          <div style={fieldStyle}>
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
          </div>
        }
      />

      <Label variant='action' as='label' style={{ marginTop: '1rem' }}>
        Adding Tables
      </Label>
      {!isNotPaused && rows.some((row) => row.selected) && (
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

      <TablePicker
        sourcePeerName={mirrorState.cdcStatus?.config?.sourceName ?? ''}
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
      <ThemedToastContainer />
    </div>
  );
}
