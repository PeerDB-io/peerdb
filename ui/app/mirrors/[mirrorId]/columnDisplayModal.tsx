'use client';
import { TableMapping } from '@/grpc_generated/flow';
import { ColumnsItem, TableColumnsResponse } from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import * as Dialog from '@radix-ui/react-dialog';
import { useEffect, useState } from 'react';
import {
  ColumnBody,
  ColumnContent,
  ColumnFooter,
  ColumnHeader,
  ColumnMessage,
  ColumnOverlay,
  ColumnRow,
  ExcludedColumnNote,
} from './styles/columnDisplayModal.styles';

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
  const [columns, setColumns] = useState<ColumnsItem[]>([]);
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
        <ColumnOverlay />
        <ColumnContent>
          {/* Header */}
          <ColumnHeader>
            <div>
              <Dialog.Title asChild>
                <Label variant='headline'>Column Details</Label>
              </Dialog.Title>
              <div style={{ marginTop: '0.5rem' }}>
                <Label variant='subheadline' colorName='lowContrast'>
                  Source: {sourceTableIdentifier}
                </Label>
                <Label variant='subheadline' colorName='lowContrast'>
                  Destination: {destinationTableIdentifier}
                </Label>
              </div>
            </div>
            <Dialog.Close asChild>
              <Button variant='normalBorderless'>
                <Icon name='close' />
              </Button>
            </Dialog.Close>
          </ColumnHeader>

          {/* Content */}
          <ColumnBody>
            {loading && (
              <ColumnMessage>
                <Label variant='body' colorName='lowContrast'>
                  Loading column information...
                </Label>
              </ColumnMessage>
            )}

            {error && (
              <ColumnMessage>
                <Label variant='body'>Error: {error}</Label>
              </ColumnMessage>
            )}

            {!loading && !error && columns.length === 0 && (
              <ColumnMessage>
                <Label variant='body' colorName='lowContrast'>
                  No columns found for this table.
                </Label>
              </ColumnMessage>
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
                    <ColumnRow key={column.name} $excluded={isExcluded}>
                      <TableCell>{column.name}</TableCell>
                      <TableCell>{column.type}</TableCell>
                      <TableCell>{column.nullable ? 'Yes' : 'No'}</TableCell>
                      <TableCell>{column.isKey ? 'Yes' : 'No'}</TableCell>
                      <TableCell>
                        <Label>{isExcluded ? 'Excluded' : 'Included'}</Label>
                      </TableCell>
                    </ColumnRow>
                  );
                })}
              </Table>
            )}

            {!loading && !error && excludedColumns.size > 0 && (
              <ExcludedColumnNote>
                <Label variant='body' colorName='lowContrast'>
                  <strong>Note:</strong> Excluded columns are shown with
                  strikethrough text and grayed out. They appear at the bottom
                  of the list.
                </Label>
              </ExcludedColumnNote>
            )}
          </ColumnBody>

          {/* Footer */}
          <ColumnFooter>
            <Dialog.Close asChild>
              <Button variant='normalBorderless'>Close</Button>
            </Dialog.Close>
          </ColumnFooter>
        </ColumnContent>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
