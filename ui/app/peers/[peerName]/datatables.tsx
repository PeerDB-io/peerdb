import { CopyButton } from '@/components/CopyButton';
import TimeLabel from '@/components/TimeComponent';
import { SlotInfo, StatInfo } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { DurationDisplay, SlotNameDisplay } from './helpers';

export const SlotTable = ({ data }: { data: SlotInfo[] }) => {
  return (
    <div style={{ height: '30%', marginTop: '2rem', marginBottom: '1rem' }}>
      <Label
        as='label'
        variant='subheadline'
        style={{ marginBottom: '1rem', fontWeight: 'bold' }}
      >
        Replication Slot Information
      </Label>
      <div
        style={{
          maxHeight: '100%',
          overflow: 'scroll',
          padding: '0.5rem',
          borderRadius: '1rem',
          boxShadow: '2px 2px 4px 2px rgba(0,0,0,0.1)',
        }}
      >
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
                    {lagInMb}
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

export const StatTable = ({ data }: { data: StatInfo[] }) => {
  return (
    <div style={{ height: '50%' }}>
      <Label
        as='label'
        variant='subheadline'
        style={{ marginBottom: '1rem', fontWeight: 'bold' }}
      >
        Stat Activity Information
      </Label>
      <div
        style={{
          maxHeight: '100%',
          overflow: 'scroll',
          padding: '0.5rem',
          borderRadius: '1rem',
          boxShadow: '2px 2px 4px 2px rgba(0,0,0,0.1)',
        }}
      >
        <Table
          header={
            <TableRow>
              {[
                'PID',
                'Duration',
                'Wait Event',
                'Wait Event Type',
                'Start Time',
                'Query',
              ].map((heading, id) => (
                <TableCell as='th' key={id}>
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
          {data.map((stat) => (
            <TableRow key={stat.pid}>
              <TableCell>
                <Label as='label' style={{ fontSize: 14 }}>
                  {stat.pid}
                </Label>
              </TableCell>
              <TableCell>
                <Label as='label' style={{ fontSize: 14 }}>
                  <DurationDisplay duration={stat.duration} />
                </Label>
              </TableCell>
              <TableCell>
                <Label as='label' style={{ fontSize: 14 }}>
                  {stat.waitEvent || 'N/A'}
                </Label>
              </TableCell>
              <TableCell>
                <Label as='label' style={{ fontSize: 14 }}>
                  {stat.waitEventType || 'N/A'}
                </Label>
              </TableCell>
              <TableCell>
                <Label as='label' style={{ fontSize: 14 }}>
                  {<TimeLabel timeVal={stat.queryStart} /> || 'N/A'}
                </Label>
              </TableCell>
              <TableCell variant='extended'>
                <div
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    fontFamily: 'monospace',
                    fontSize: 13,
                  }}
                >
                  {stat.query}
                  <CopyButton text={stat.query} />
                </div>
              </TableCell>
            </TableRow>
          ))}
        </Table>
      </div>
    </div>
  );
};
