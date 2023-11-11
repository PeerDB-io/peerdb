import { Meta, StoryObj } from '@storybook/react';
import { Badge } from '../Badge';
import { Button } from '../Button';
import { Checkbox } from '../Checkbox';
import { Icon } from '../Icon';
import { Label } from '../Label';
import { TableCell } from './TableCell';
import { TableRow } from './TableRow';

export default {
  title: 'Components / Table / TableRow',
  component: TableRow,
  render: () => (
    <table>
      <tbody>
        <TableRow>
          <TableCell variant='button'>
            <Checkbox />
          </TableCell>
          <TableCell variant='extended'>
            <Label>Label</Label>
          </TableCell>
          <TableCell>
            <Label>Label</Label>
          </TableCell>
          <TableCell>
            <Label>Label</Label>
          </TableCell>
          <TableCell>
            <Label>Label</Label>
          </TableCell>
          <TableCell>
          </TableCell>
          <TableCell>
            <Badge variant='positive'>Active</Badge>
          </TableCell>
          <TableCell variant='button'>
            <Button>
              <Icon name='more_horiz' />
            </Button>
          </TableCell>
        </TableRow>
      </tbody>
    </table>
  ),
} satisfies Meta<typeof TableRow>;

type Story = StoryObj<typeof TableRow>;
export const Default: Story = {};
