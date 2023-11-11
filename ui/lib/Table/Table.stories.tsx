import { Meta, StoryObj } from '@storybook/react';
import { Badge } from '../Badge';
import { Button } from '../Button';
import { Checkbox } from '../Checkbox';
import { Icon } from '../Icon';
import { Label } from '../Label';
import { SearchField } from '../SearchField';
import { Table } from './Table';
import { TableCell } from './TableCell';
import { TableRow } from './TableRow';

export default {
  title: 'Components / Table',
  component: Table,
  tags: ['autodocs'],
  render: (props) => (
    <Table
      {...props}
      title={<Label>Table title</Label>}
      toolbar={{
        left: (
          <>
            <Button variant='normalBorderless'>
              <Icon name='chevron_left' />
            </Button>
            <Button variant='normalBorderless'>
              <Icon name='chevron_right' />
            </Button>
            <Button variant='normalBorderless'>
              <Icon name='refresh' />
            </Button>
            <Button variant='normalBorderless'>
              <Icon name='help' />
            </Button>
            <Button variant='normalBorderless' disabled>
              <Icon name='download' />
            </Button>
          </>
        ),
        right: (
          <>
            <SearchField placeholder='Search' />
            <Button variant='normalSolid'>New item</Button>
          </>
        ),
      }}
      header={
        <TableRow>
          <TableCell as='th' variant='button'>
            <Checkbox variant='mixed' defaultChecked />
          </TableCell>
          <TableCell as='th' variant='button'>
            <Button>
              <Icon name='more_horiz' />
            </Button>
          </TableCell>
        </TableRow>
      }
    >
      <TableRow>
        <TableCell variant='button'>
          <Checkbox />
        </TableCell>
        <TableCell variant='extended'>
          <Label>
            Lorem ipsum dolor, sit amet consectetur adipisicing elit. Est
            numquam at accusantium quis. Corrupti cum, alias magnam placeat
            dolorem nemo officia ipsam vel veritatis inventore itaque
            repellendus suscipit laborum error.
          </Label>
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
          <Label>Label</Label>
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
          <Label>Label</Label>
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
          <Label>Label</Label>
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
    </Table>
  ),
} satisfies Meta<typeof Table>;

type Story = StoryObj<typeof Table>;
export const Default: Story = {};
