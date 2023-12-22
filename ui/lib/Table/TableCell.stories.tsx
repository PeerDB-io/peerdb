import { Meta, StoryObj } from '@storybook/react';
import { Checkbox } from '../Checkbox';
import { Label } from '../Label';
import { TableCell } from './TableCell';

export default {
  title: 'Components / Table / TableCell',
  component: TableCell,
} satisfies Meta<typeof TableCell>;

type Story = StoryObj<typeof TableCell>;
export const ButtonCell: Story = {
  args: {
    variant: 'button',
  },
  render: (props) => (
    <TableCell {...props}>
      <Checkbox />
    </TableCell>
  ),
};

export const NormalCell: Story = {
  args: {
    variant: 'normal',
  },
  render: (props) => <TableCell {...props}></TableCell>,
};

export const ExtendedCell: Story = {
  args: {
    variant: 'extended',
  },
  render: (props) => (
    <TableCell {...props}>
      <Label>Label</Label>
    </TableCell>
  ),
};
