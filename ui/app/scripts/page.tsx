import NewButton from '@/components/NewButton';
import { Label } from '@/lib/Label/Label';
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
        rowGap: '2rem',
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
        <Label>Some information regarding scripts should be shown here</Label>
      </div>
      <ScriptsTable scripts={scripts} />
    </div>
  );
};

export default ScriptsPage;
