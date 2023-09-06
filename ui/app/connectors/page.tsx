import { Badge } from '@/lib/Badge';
import { Button } from '@/lib/Button';
import { Checkbox } from '@/lib/Checkbox';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { LayoutMain } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { SearchField } from '@/lib/SearchField';
import { Select } from '@/lib/Select';
import { Table, TableCell, TableRow } from '@/lib/Table';
import Link from 'next/link';
import { Header } from '../../lib/Header';

const Badges = [
  <Badge variant='positive' key={1}>
    <Icon name='play_circle' />
    Active
  </Badge>,
  <Badge variant='warning' key={1}>
    <Icon name='pause_circle' />
    Paused
  </Badge>,
  <Badge variant='destructive' key={1}>
    <Icon name='dangerous' />
    Broken
  </Badge>,
  <Badge variant='normal' key={1}>
    <Icon name='pending' />
    Incomplete
  </Badge>,
];


async function fetchPeers() {

}


const ExampleTable = ({ title }: { title: string }) => (
  <Table
    title={<Label variant='headline'>{title}</Label>}
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
      right: <SearchField placeholder='Search' />,
    }}
    header={
      <TableRow>
        <TableCell as='th' variant='button'>
          <Checkbox variant='mixed' defaultChecked />
        </TableCell>
        <TableCell as='th'>
          <Select placeholder='Select' />
        </TableCell>
        <TableCell as='th'>
          <Select placeholder='Select' />
        </TableCell>
        <TableCell as='th'>
          <Select placeholder='Select' />
        </TableCell>
        <TableCell as='th'>
          <Select placeholder='Select' />
        </TableCell>
        <TableCell as='th'>
          <Select placeholder='Select' />
        </TableCell>
        <TableCell as='th'>
          <Select placeholder='Select' />
        </TableCell>
        <TableCell as='th' variant='button'>
          <Button>
            <Icon name='more_horiz' />
          </Button>
        </TableCell>
      </TableRow>
    }
  >
    {Array(8)
      .fill(null)
      .map((_, index) => (
        <TableRow key={index}>
          <TableCell variant='button'>
            <Checkbox />
          </TableCell>
          <TableCell variant='extended'>
            <Label as={Link} href='/connectors/edit/TestConnector'>
              Test connector
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
          <TableCell>{Badges[index % Badges.length]}</TableCell>
          <TableCell variant='button'>
            <Button>
              <Icon name='more_horiz' />
            </Button>
          </TableCell>
        </TableRow>
      ))}
  </Table>
);

export default function Connectors() {
  return (
    <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
      <Panel>
        <Header
          variant='title2'
          slot={
            <Button as={Link} href={'/connectors/create'} variant='normalSolid'>
              New connector
            </Button>
          }
        >
          Connectors
        </Header>
      </Panel>
      <Panel>
        <ExampleTable title='All connectors' />
      </Panel>
    </LayoutMain>
  );
}
