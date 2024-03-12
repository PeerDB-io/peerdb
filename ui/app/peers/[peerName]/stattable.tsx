'use client';
import { CopyButton } from '@/components/CopyButton';
import TimeLabel from '@/components/TimeComponent';
import { StatInfo } from '@/grpc_generated/route';
import { Label } from '@/lib/Label';
import { SearchField } from '@/lib/SearchField';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { useMemo, useState } from 'react';
import { DurationDisplay } from './helpers';
import { tableStyle } from './style';

const StatTable = ({ data }: { data: StatInfo[] }) => {
  const [search, setSearch] = useState('');
  const filteredData = useMemo(() => {
    return data.filter((stat) => {
      return stat.query.toLowerCase().includes(search.toLowerCase());
    });
  }, [data, search]);

  return (
    <div style={{ minHeight: '10%' }}>
      <Label
        as='label'
        variant='subheadline'
        style={{ marginBottom: '1rem', fontWeight: 'bold' }}
      >
        Stat Activity Information
      </Label>
      <div style={tableStyle}>
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
          toolbar={{
            left: <></>,
            right: (
              <SearchField
                placeholder='Search by query'
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  setSearch(e.target.value)
                }
              />
            ),
          }}
        >
          {filteredData.map((stat) => (
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
                <TimeLabel timeVal={stat.queryStart} fontSize={14} />
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

export default StatTable;
