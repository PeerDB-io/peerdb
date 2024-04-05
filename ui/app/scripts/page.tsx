import NewButton from '@/components/NewButton';
import { Label } from '@/lib/Label/Label';
import Link from 'next/link';
import { ScriptsType } from '../dto/ScriptsDTO';
import prisma from '../utils/prisma';
import ScriptsTable from './list';
export const dynamic = 'force-dynamic';
export const revalidate = 5;

const ScriptsPage = async () => {
  const existingScripts = await prisma.scripts.findMany();
  const scripts: ScriptsType[] = existingScripts.map((script) => ({
    ...script,
    source: script.source.toString(),
  }));
  return (
    <div
      style={{
        padding: '1rem',
        display: 'flex',
        flexDirection: 'column',
        width: '100%',
        rowGap: '1rem',
      }}
    >
      <div>
        <div
          style={{
            display: 'flex',
            width: '100%',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}
        >
          <Label variant='title3'>Scripts</Label>
          <NewButton targetPage='/scripts/new' buttonText='New script' />
        </div>
      </div>
      <div>
        <Label>
          This is a console for writing Lua scripts for PeerDB queue peers.
        </Label>
        <Label as='label' style={{ display: 'block' }}>
          These scripts allow you to specify how change-data information from
          PostgreSQL will look like in your messages, via the PeerDB Lua API.
        </Label>
        <Label
          as={Link}
          target='_blank'
          style={{
            color: 'teal',
            cursor: 'pointer',
            width: 'fit-content',
            display: 'block',
          }}
          href={`https://docs.peerdb.io/lua/reference`}
        >
          Learn more about PeerDB Lua scripting
        </Label>
      </div>
      <ScriptsTable scripts={scripts} />
    </div>
  );
};

export default ScriptsPage;
