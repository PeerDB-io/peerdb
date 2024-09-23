import { MirrorType } from '@/app/dto/MirrorsDTO';
import { GetPeerDBClickHouseMode } from '@/peerdb-env/allowed_targets';
import { NextRequest } from 'next/server';
export const dynamic = 'force-dynamic';
export async function GET(_: NextRequest) {
  const cards = [
    {
      title: MirrorType.CDC,
      description:
        'Change-data Capture or CDC refers to replication of changes on the source table to the target table with initial load. This is recommended.',
      link: 'https://docs.peerdb.io/usecases/Real-time%20CDC/overview',
    },
    {
      title: MirrorType.QRep,
      description:
        'Query Replication allows you to specify a set of rows to be synced via a SELECT query. Useful for replicating views and tables without primary keys.',
      link: 'https://docs.peerdb.io/usecases/Streaming%20Query%20Replication/overview',
    },
  ];
  const xminCard = {
    title: MirrorType.XMin,
    description:
      'XMIN mode uses the xmin system column of PostgreSQL as a watermark column for replication.',
    link: 'https://docs.peerdb.io/sql/commands/create-mirror#xmin-query-replication',
  };

  if (!GetPeerDBClickHouseMode()) {
    cards.push(xminCard);
  }
  return new Response(JSON.stringify(cards));
}
