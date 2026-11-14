import {
  BigQueryReplicationMethod,
  bigQueryReplicationMethodFromJSON,
  FlowConnectionConfigs,
} from '@/grpc_generated/flow';

function bigqueryCdcValues(mirrorConfig: FlowConnectionConfigs | undefined) {
  const bigqueryCdc = mirrorConfig?.bigqueryCdcConfig;
  if (!bigqueryCdc) {
    return [];
  }
  const queryCdc = bigqueryCdc.queryCdc;
  const orDefault = (value: number | undefined, unit: string) =>
    value ? `${value} ${unit}` : 'Default';
  return [
    {
      value:
        bigQueryReplicationMethodFromJSON(bigqueryCdc.replicationMethod) ===
        BigQueryReplicationMethod.BIGQUERY_REPLICATION_METHOD_QUERY
          ? 'Query'
          : 'Events',
      label: 'BigQuery Replication Method',
    },
    {
      value: orDefault(queryCdc?.pullSyncParallelism, 'table(s)'),
      label: 'Pull Sync Parallelism',
    },
    {
      value: orDefault(queryCdc?.safetyLagSeconds, 'seconds'),
      label: 'Safety Lag',
    },
    {
      value: orDefault(queryCdc?.maxQueryWindowSeconds, 'seconds'),
      label: 'Max Query Window',
    },
  ];
}

export default function MirrorValues(
  mirrorConfig: FlowConnectionConfigs | undefined
) {
  return [
    ...bigqueryCdcValues(mirrorConfig),
    {
      value: `${mirrorConfig?.maxBatchSize} rows`,
      label: 'Pull Batch Size',
    },
    {
      value: `${mirrorConfig?.snapshotNumRowsPerPartition} rows`,
      label: 'Snapshot Rows Per Partition',
    },
    {
      value: `${mirrorConfig?.snapshotNumTablesInParallel} table(s)`,
      label: 'Snapshot Tables In Parallel',
    },
    {
      value: `${mirrorConfig?.snapshotMaxParallelWorkers} worker(s)`,
      label: 'Snapshot Parallel Workers',
    },
    {
      value: mirrorConfig?.softDeleteColName
        ? `Enabled (${mirrorConfig?.softDeleteColName})`
        : 'Disabled',
      label: 'Soft Delete',
    },
    {
      value: mirrorConfig?.script,
      label: 'Script',
    },
    {
      value:
        mirrorConfig?.publicationName ||
        `peerflow_pub_${mirrorConfig?.flowJobName}`,
      label: 'Publication Name',
    },
  ];
}
