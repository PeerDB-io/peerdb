'use client';
import { TableMapping } from '@/grpc_generated/flow';
import { TableColumnsResponse } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Table, TableCell } from '@/lib/Table';
import * as Dialog from '@radix-ui/react-dialog';
import { TableRow } from '@tremor/react';
import { useEffect, useState } from 'react';

interface ColumnDisplayModalProps {
  isOpen: boolean;
  onClose: () => void;
  sourceTableIdentifier: string;
  destinationTableIdentifier: string;
  tableMapping: TableMapping | null;
  sourcePeerName: string;
}

export default function ColumnDisplayModal({
  isOpen,
  onClose,
  sourceTableIdentifier,
  destinationTableIdentifier,
  tableMapping,
  sourcePeerName,
}: ColumnDisplayModalProps) {
  const [columns, setColumns] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (isOpen && sourceTableIdentifier && sourcePeerName) {
      const fetchTableColumns = async () => {
        try {
          setLoading(true);
          setError(null);

          // Parse schema and table name from sourceTableIdentifier (e.g., "public.users")
          const [schemaName, tableName] = sourceTableIdentifier.split('.');

          if (!schemaName || !tableName) {
            throw new Error('Invalid table identifier format');
          }

          const response: TableColumnsResponse = await fetch(
            `/api/v1/peers/columns?peer_name=${encodeURIComponent(
              sourcePeerName
            )}&schema_name=${encodeURIComponent(schemaName)}&table_name=${encodeURIComponent(tableName)}`,
            {
              cache: 'no-store',
            }
          ).then((res) => {
            if (!res.ok) {
              throw new Error('Failed to fetch columns');
            }
            return res.json();
          });

          setColumns(response.columns || []);
        } catch (err) {
          console.error('Error fetching columns:', err);
          setError(
            err instanceof Error ? err.message : 'Failed to fetch columns'
          );
          setColumns([]);
        } finally {
          setLoading(false);
        }
      };

      fetchTableColumns();
    }
  }, [isOpen, sourceTableIdentifier, sourcePeerName]);

  const excludedColumns = new Set(tableMapping?.exclude || []);

  const sortedColumns = [...columns].sort((a, b) => {
    const aExcluded = excludedColumns.has(a.name);
    const bExcluded = excludedColumns.has(b.name);

    // Non-excluded columns first, then excluded columns
    if (aExcluded !== bExcluded) {
      return aExcluded ? 1 : -1;
    }

    // Within each group, sort alphabetically
    return a.name.localeCompare(b.name);
  });

  return (
    <Dialog.Root open={isOpen} onOpenChange={onClose}>
      <Dialog.Portal>
        <Dialog.Overlay className='fixed inset-0 z-50' />
        <Dialog.Content className='fixed top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 bg-white dark:bg-gray-900 rounded-lg shadow-xl max-w-4xl w-full max-h-[80vh] overflow-hidden z-50'>
          {/* Header */}
          <div className='flex items-center justify-between p-6 border-b'>
            <div>
              <Dialog.Title asChild>
                <Label variant='headline' className='text-xl font-semibold'>
                  Column Details
                </Label>
              </Dialog.Title>
              <div className='mt-2 space-y-1'>
                <Label variant='subheadline' colorName='lowContrast'>
                  Source: {sourceTableIdentifier}
                </Label>
                <Label variant='subheadline' colorName='lowContrast'>
                  Destination: {destinationTableIdentifier}
                </Label>
              </div>
            </div>
            <Dialog.Close asChild>
              <Button variant='normalBorderless' className='p-2'>
                <Icon name='close' />
              </Button>
            </Dialog.Close>
          </div>

          {/* Content */}
          <div
            className='p-6 overflow-auto max-h-[60vh]'
            style={{ height: '30em' }}
          >
            {loading && (
              <div className='text-center py-8'>
                <Label variant='body' colorName='lowContrast'>
                  Loading column information...
                </Label>
              </div>
            )}

            {error && (
              <div className='text-center py-8'>
                <Label variant='body' className='text-red-500'>
                  Error: {error}
                </Label>
              </div>
            )}

            {!loading && !error && columns.length === 0 && (
              <div className='text-center py-8'>
                <Label variant='body' colorName='lowContrast'>
                  No columns found for this table.
                </Label>
              </div>
            )}

            {!loading && !error && columns.length > 0 && (
              <Table
                header={
                  <TableRow>
                    <TableCell>Column Name</TableCell>
                    <TableCell>Type</TableCell>
                    <TableCell>Nullable</TableCell>
                    <TableCell>Primary Key</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                }
              >
                {sortedColumns.map((column) => {
                  const isExcluded = excludedColumns.has(column.name);
                  return (
                    <TableRow
                      key={column.name}
                      className={
                        isExcluded
                          ? 'opacity-60 bg-gray-50 dark:bg-gray-800'
                          : ''
                      }
                    >
                      <TableCell className={isExcluded ? 'line-through' : ''}>
                        {column.name}
                      </TableCell>
                      <TableCell className={isExcluded ? 'line-through' : ''}>
                        {column.type}
                      </TableCell>
                      <TableCell className={isExcluded ? 'line-through' : ''}>
                        {column.nullable ? 'Yes' : 'No'}
                      </TableCell>
                      <TableCell className={isExcluded ? 'line-through' : ''}>
                        {column.primaryKey ? 'Yes' : 'No'}
                      </TableCell>
                      <TableCell>
                        {isExcluded ? (
                          <Label className='text-red-600 dark:text-red-400 font-medium'>
                            Excluded
                          </Label>
                        ) : (
                          <Label className='text-green-600 dark:text-green-400 font-medium'>
                            Included
                          </Label>
                        )}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </Table>
            )}

            {!loading && !error && excludedColumns.size > 0 && (
              <div className='text-xs text-gray-500 dark:text-gray-400 pt-4 border-t mt-4'>
                <Label variant='body' colorName='lowContrast'>
                  <strong>Note:</strong> Excluded columns are shown with
                  strikethrough text and grayed out. They appear at the bottom
                  of the list.
                </Label>
              </div>
            )}
          </div>

          {/* Footer */}
          <div className='flex justify-end p-6 border-t'>
            <Dialog.Close asChild>
              <Button variant='normalBorderless'>Close</Button>
            </Dialog.Close>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
