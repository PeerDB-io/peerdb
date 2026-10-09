import { TableMapping } from '@/grpc_generated/flow';
import { DBType, dBTypeFromJSON } from '@/grpc_generated/peers';

// isMongoSource reports whether a peer of the given type is a MongoDB source (any variant: every
// one is DBType.MONGO), whose tables can carry MongoDB specific settings.
export function isMongoSource(sourceType?: DBType): boolean {
  return (
    sourceType !== undefined && dBTypeFromJSON(sourceType) === DBType.MONGO
  );
}

// MongoConfigured is the part of a TableMapping that carries the MongoDB specific settings.
type MongoConfigured = Pick<TableMapping, 'mongoConfig'>;

// deletePreimageEnabled reports whether the mapping's delete events carry the document as it
// stood before the delete.
export function deletePreimageEnabled(mapping: MongoConfigured): boolean {
  return mapping.mongoConfig?.deletePreimage ?? false;
}

// withDeletePreimage returns a copy of the mapping with delete pre-images enabled or not. A
// mapping without any MongoDB specific setting enabled carries no settings at all.
export function withDeletePreimage<T extends MongoConfigured>(
  mapping: T,
  enabled: boolean
): T {
  return {
    ...mapping,
    mongoConfig: enabled
      ? { ...mapping.mongoConfig, deletePreimage: true }
      : undefined,
  };
}
