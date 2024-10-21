'use client';
import DropDialog from '@/components/DropDialog';
import { Script } from '@/grpc_generated/route';
import { Button } from '@/lib/Button/Button';
import { Label } from '@/lib/Label/Label';
import { SearchField } from '@/lib/SearchField';
import { TableCell } from '@/lib/Table';
import { Table } from '@/lib/Table/Table';
import { TableRow } from '@tremor/react';
import Image from 'next/image';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { useEffect, useMemo, useState } from 'react';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

const LanguageIcon = (language: string) => {
  switch (language.toLowerCase()) {
    case 'lua':
      return '/svgs/lua.svg';
    default:
      return '/svgs/lua.svg';
  }
};

const ScriptsTable = ({ scripts }: { scripts: Script[] }) => {
  const router = useRouter();
  const [searchQuery, setSearchQuery] = useState('');
  const displayedScripts = useMemo(() => {
    return scripts.filter((script) => script.name.includes(searchQuery));
  }, [scripts, searchQuery]);

  // This is a hack to ensure this table
  // shows updated data when landing from /scripts/new
  // after adding a script
  // router.push/replace does not seem to invalidate cache for /scripts
  // https://github.com/vercel/next.js/discussions/54075
  useEffect(() => {
    router.refresh();
  }, [router]);
  return (
    <>
      <Table
        toolbar={{
          left: (
            <SearchField
              placeholder='Search by script name'
              onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                setSearchQuery(e.target.value)
              }
            />
          ),
        }}
        header={
          <TableRow>
            {['Name', 'Language'].map((heading) => {
              return (
                <TableCell key={heading} as='th'>
                  {heading}
                </TableCell>
              );
            })}
          </TableRow>
        }
      >
        {displayedScripts.map((script) => {
          return (
            <TableRow key={script.id}>
              <TableCell as='td'>{script.name}</TableCell>
              <TableCell as='td'>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    width: 'fit-content',
                  }}
                >
                  <Image
                    src={LanguageIcon(script.lang)}
                    alt='lang'
                    width={20}
                    height={10}
                  />
                  <Label>{script.lang.toUpperCase()}</Label>
                </div>
              </TableCell>
              <TableCell as='td'>
                <Button
                  as={Link}
                  href={`/scripts/new?scriptid=${script.id}`}
                  style={{ width: 'fit-content' }}
                  variant='normal'
                >
                  View and edit
                </Button>
              </TableCell>
              <TableCell as='td'>
                <DropDialog
                  mode={'SCRIPT'}
                  dropArgs={{ scriptId: script.id }}
                />
              </TableCell>
            </TableRow>
          );
        })}
      </Table>
      <ToastContainer />
    </>
  );
};

export default ScriptsTable;
