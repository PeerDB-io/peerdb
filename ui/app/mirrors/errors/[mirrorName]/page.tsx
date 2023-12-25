import { AlertErr } from '@/app/dto/MirrorsDTO';
import prisma from '@/app/utils/prisma';
import TimeLabel from '@/components/TimeComponent';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';

type MirrorErrorProps = {
  params: { mirrorName: string };
};

const MirrorError = async ({ params: { mirrorName } }: MirrorErrorProps) => {
  const mirrorErrors: AlertErr[] = await prisma.flow_errors.findMany({
    where: {
      flow_name: mirrorName,
      error_type: 'error',
    },
    distinct: ['error_message'],
    orderBy: {
      error_timestamp: 'desc',
    },
  });

  return (
    <div style={{ padding: '2rem' }}>
      <Label variant='title2'>Error Log</Label>
      <hr></hr>
      <div style={{ marginTop: '1rem' }}>
        <Label variant='body'>
          <b>Mirror name</b>:
        </Label>
        <Label variant='body'>{mirrorName}</Label>
        <div
          style={{
            fontSize: 15,
            marginTop: '1rem',
            width: '100%',
            border: '1px solid rgba(0,0,0,0.1)',
            padding: '1rem',
            borderRadius: '1rem',
          }}
        >
          <Table
            header={
              <TableRow style={{ textAlign: 'left' }}>
                <TableCell>Type</TableCell>
                <Label as='label' style={{ fontSize: 15 }}>
                  Time
                </Label>
                <TableCell>Message</TableCell>
              </TableRow>
            }
          >
            {mirrorErrors.map((mirrorError) => (
              <TableRow key={mirrorError.error_message}>
                <TableCell style={{ color: '#F45156', width: '10%' }}>
                  {mirrorError.error_type.toUpperCase()}
                </TableCell>
                <TableCell style={{ width: '20%' }}>
                  <TimeLabel
                    fontSize={14}
                    timeVal={mirrorError.error_timestamp.toLocaleString()}
                  />
                </TableCell>
                <TableCell style={{ width: '70%', fontSize: 13 }}>
                  {mirrorError.error_message}
                </TableCell>
              </TableRow>
            ))}
          </Table>
        </div>
      </div>
    </div>
  );
};

export default MirrorError;
