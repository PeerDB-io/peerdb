import { SlotInfo } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { SlotNameDisplay } from './helpers';
import { tableStyle } from './style';

const SlotTable = ({ data }: { data: SlotInfo[] }) => {
  return (
    <div style={{ height: '30%', marginTop: '2rem', marginBottom: '1rem' }}>
      <Label
        as='label'
        variant='subheadline'
        style={{ marginBottom: '1rem', fontWeight: 'bold' }}
      >
        Replication Slot Information
      </Label>
      <div style={tableStyle}>
        <Table
          header={
            <TableRow>
              {[
                'Slot Name',
                'Active',
                'Redo LSN',
                'Restart LSN',
                'Lag (In MB)',
              ].map((heading, index) => (
                <TableCell as='th' key={index}>
                  <Label
                    as='label'
                    style={{ fontWeight: 'bold', fontSize: 14 }}
                  >
                    {heading}
                  </Label>
                </TableCell>
              ))}
            </TableRow>
          }
        >
          {data.map(({ slotName, active, redoLSN, restartLSN, lagInMb }) => {
            return (
              <TableRow key={slotName}>
                <TableCell>
                  <SlotNameDisplay slotName={slotName} />
                </TableCell>
                <TableCell>
                  <Label as='label' style={{ fontSize: 14 }}>
                    {active ? 'Yes' : 'No'}
                  </Label>
                </TableCell>
                <TableCell>
                  <Label as='label' style={{ fontSize: 14 }}>
                    {redoLSN}
                  </Label>
                </TableCell>
                <TableCell>
                  <Label as='label' style={{ fontSize: 14 }}>
                    {restartLSN}
                  </Label>
                </TableCell>
                <TableCell>
                  <Label as='label' style={{ fontSize: 14 }}>
                    {lagInMb < 0 ? 0 : lagInMb}
                  </Label>
                </TableCell>
              </TableRow>
            );
          })}
        </Table>
      </div>
    </div>
  );
};

export default SlotTable;
