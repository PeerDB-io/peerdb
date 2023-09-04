'use client';
import { PolymorphicComponentProps } from '../types';
import { BaseTableCell, TableCellVariant } from './TableCell.styles';

type TableCellProps = {
  variant?: TableCellVariant;
};
export function TableCell<AsProp extends React.ElementType>({
  variant = 'normal',
  ...cellProps
}: PolymorphicComponentProps<AsProp, TableCellProps>) {
  return <BaseTableCell {...cellProps} $variant={variant} />;
}
