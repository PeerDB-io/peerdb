import { QualifiedTable } from '@/grpc_generated/flow';
import { DBType } from '@/grpc_generated/peers';

export function displayQualifiedTable(qt: QualifiedTable | undefined): string {
  if (!qt) return '';
  return qt.namespace ? `${qt.namespace}.${qt.table}` : qt.table;
}

export function qualifiedTableFromParts(
  namespace: string,
  table: string
): QualifiedTable {
  return { namespace, table };
}

// for dotted source identifiers (schema.table): split at the first dot
export function qualifiedTableFromDottedName(name: string): QualifiedTable {
  const dotIndex = name.indexOf('.');
  if (dotIndex === -1) {
    return { namespace: '', table: name };
  }
  return {
    namespace: name.slice(0, dotIndex),
    table: name.slice(dotIndex + 1),
  };
}

// peer types arrive either as enum numbers or as JSON string names ("CLICKHOUSE"),
// depending on whether they came through a fetch response
function isDBType(peerType: DBType | undefined, target: DBType): boolean {
  return (
    peerType != null &&
    (peerType === target || peerType.toString() === DBType[target])
  );
}

const tableOnlyDestinations = [
  DBType.CLICKHOUSE,
  DBType.KAFKA,
  DBType.PUBSUB,
  DBType.ELASTICSEARCH,
  DBType.S3,
];

// Parses the free-text destination table input into a QualifiedTable:
// - table-only destinations (ClickHouse, Kafka, PubSub, Elasticsearch, S3): whole text is the table
// - BigQuery (dataset.table): split at the last dot, dataset may itself contain dots
// - EventHubs (namespace.eventhub.partition_key_column): split at the first dot,
//   "eventhub.partition_key_column" is packed into the table component
// - otherwise (schema.table): split at the first dot; no dot means table only
export function parseDestinationInput(
  text: string,
  peerType?: DBType
): QualifiedTable {
  if (tableOnlyDestinations.some((dbType) => isDBType(peerType, dbType))) {
    return { namespace: '', table: text };
  }
  const splitAt = isDBType(peerType, DBType.BIGQUERY)
    ? text.lastIndexOf('.')
    : text.indexOf('.');
  if (splitAt === -1) {
    return { namespace: '', table: text };
  }
  return { namespace: text.slice(0, splitAt), table: text.slice(splitAt + 1) };
}
