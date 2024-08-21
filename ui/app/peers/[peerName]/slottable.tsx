'use client';
import { SlotInfo } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { useEffect, useState } from 'react';
import { getSlotData, SlotNameDisplay } from './helpers';
import { tableStyle } from './style';

const SlotTable = ({ peerName }: { peerName: string }) => {
  const [data, setData] = useState<SlotInfo[]>([]);

  useEffect(() => {
    getSlotData(peerName).then((slots) => setData(slots));
  }, [peerName]);

  if (!data || data.length === 0) {
    return (
      <div
        style={{ minHeight: '10%', marginTop: '2rem', marginBottom: '2rem' }}
      >
        <Label
          as='label'
          variant='subheadline'
          style={{ marginBottom: '1rem', fontWeight: 'bold' }}
        >
          Replication Slot Information
        </Label>
        <ProgressCircle variant='determinate_progress_circle' />
      </div>
    );
  }
  return (
    <div style={{ minHeight: '10%', marginTop: '2rem', marginBottom: '2rem' }}>
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
