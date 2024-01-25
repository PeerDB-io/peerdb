// BOILERPLATE
'use client';

import { TableMapRow } from '@/app/dto/MirrorsDTO';
import { DBType } from '@/grpc_generated/peers';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { useState } from 'react';
import TableMapping from '../../create/cdc/tablemapping';

type EditMirrorProps = {
  params: { mirrorId: string };
};
const EditMirror = ({ params: { mirrorId } }: EditMirrorProps) => {
  const [rows, setRows] = useState<TableMapRow[]>([]);
  // todo: use mirrorId to query flows table/temporal and get config
  // you will have to decode the config to get the table mapping. see: /mirrors/page.tsx
  return (
    <div>
      <Label variant='title3'>Edit {mirrorId}</Label>

      {/* todo: add a prop to this component called alreadySelectedTables and pass the table mapping.
        Then, at the place where we're blurring out tables on the condition of
        pkey/replica identity (schemabox.tsx), extend the condition to include the case where table is in alreadySelectedTables */}
      <TableMapping
        sourcePeerName='postgres_local' // get this from config
        peerType={DBType.SNOWFLAKE} // this is destination peer type. get it from config
        rows={rows}
        setRows={setRows}
      />
      <Button
        style={{ marginTop: '1rem', width: '8%', height: '2.5rem' }}
        variant='normalSolid'
      >
        Update tables
      </Button>
    </div>
  );
};

export default EditMirror;
