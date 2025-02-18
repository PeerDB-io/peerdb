'use client';
import DropDialog from '@/components/DropDialog';
import { Cert } from '@/grpc_generated/route';
import { SearchField } from '@/lib/SearchField';
import { TableCell } from '@/lib/Table';
import { Table } from '@/lib/Table/Table';
import { TableRow } from '@tremor/react';
import { useMemo, useState } from 'react';
import { ToastContainer } from 'react-toastify';

export default function CertsTable({ certs }: { certs: Cert[] }) {
  const [searchQuery, setSearchQuery] = useState('');
  const displayedCerts = useMemo(
    () => certs.filter((cert) => cert.name.includes(searchQuery)),
    [certs, searchQuery]
  );

  return (
    <>
      <Table
        toolbar={{
          left: (
            <SearchField
              placeholder='Search by certificate name'
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setSearchQuery(e.target.value)
              }
            />
          ),
        }}
        header={
          <TableRow>
            <TableCell key='Name' as='th'>
              Name
            </TableCell>
          </TableRow>
        }
      >
        {displayedCerts.map((cert) => (
          <TableRow key={cert.id}>
            <TableCell as='td'>{cert.name}</TableCell>
            <TableCell as='td'>
              <DropDialog mode={'CERT'} dropArgs={{ certId: cert.id }} />
            </TableCell>
          </TableRow>
        ))}
      </Table>
      <ToastContainer />
    </>
  );
}
