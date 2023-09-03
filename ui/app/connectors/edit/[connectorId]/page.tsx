'use client';

import { Badge } from '@/lib/Badge';
import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { TrackerChart } from '@/lib/Chart';
import { Checkbox } from '@/lib/Checkbox';
import { Header } from '@/lib/Header';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import {
  LayoutMain,
  LayoutRightSidebar,
  Row,
  RowWithToggleGroup,
} from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { SearchField } from '@/lib/SearchField';
import { Select } from '@/lib/Select';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { ToggleGroup, ToggleGroupItem } from '@/lib/Toggle';
import { Color } from '@tremor/react';
import { useState } from 'react';

const weekdays = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];

interface Tracker {
  color: Color;
  tooltip: string;
}

const weekData = weekdays.map<Tracker>((weekDay) => ({
  tooltip: weekDay,
  color: Math.random() > 0.5 ? 'blue' : 'gray',
}));

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

const ExampleTable = ({ title }: { title: string }) => (
  <Table
    title={() => <Label variant='headline'>{title}</Label>}
    toolbar={{
      left: () => (
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
      right: () => <SearchField placeholder='Search' />,
    }}
    header={() => (
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
    )}
  >
    {Array(8)
      .fill(null)
      .map((_, index) => (
        <TableRow key={index}>
          <TableCell variant='button'>
            <Checkbox />
          </TableCell>
          <TableCell variant='extended'>
            <Label>Lorem</Label>
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

type EditMirrorProps = {
  params: { connectorId: string };
};
export default function EditConnector({
  params: { connectorId },
}: EditMirrorProps) {
  const [logsOpen, setLogsOpen] = useState(false);
  return (
    <>
      <LayoutMain alignSelf='flex-start' justifySelf='flex-start' width='full'>
        <Panel>
          <Header
            variant='title2'
            slot={() => (
              <ButtonGroup>
                <Button>Disable connector</Button>
                <Button onClick={() => setLogsOpen(!logsOpen)}>See logs</Button>
                <Button>Sync now</Button>
                <Button variant='normalSolid'>Edit connector</Button>
              </ButtonGroup>
            )}
          >
            {connectorId}
          </Header>
          <Label colorName='lowContrast' variant='footnote'>
            Created on Jan 2023
          </Label>
        </Panel>
        <Panel>
          <div className='flex flex-row flex-nowrap'>
            <Row
              className='flex-1'
              preTitle={() => 'Status'}
              title={() => 'Running'}
            />
            <Row
              className='flex-1'
              preTitle={() => 'Mirror'}
              title={() => 'Label'}
            />
            <Row
              className='flex-1'
              preTitle={() => 'Source'}
              title={() => 'Label'}
            />
            <Row
              className='flex-1'
              preTitle={() => 'Destination'}
              title={() => 'Label'}
            />
          </div>
          <div className='flex flex-row flex-nowrap'>
            <Row
              className='flex-1'
              preTitle={() => 'Last sync'}
              title={() => '41 min'}
            />
            <Row
              className='flex-1'
              preTitle={() => 'Next sync in'}
              title={() => '19 min'}
            />
            <Row
              className='flex-1'
              preTitle={() => 'Rows synced'}
              title={() => '27%'}
            />
            <Row
              className='flex-1'
              preTitle={() => 'Avg. sync time'}
              title={() => '8.2min'}
            />
          </div>
          <TrackerChart
            data={weekData}
            top={() => (
              <RowWithToggleGroup
                label={() => <Label>Sync history</Label>}
                action={() => (
                  <ToggleGroup defaultValue='item1'>
                    <ToggleGroupItem value='item1'>Month</ToggleGroupItem>
                    <ToggleGroupItem value='item2'>Week</ToggleGroupItem>
                    <ToggleGroupItem value='item3'>Day</ToggleGroupItem>
                  </ToggleGroup>
                )}
              />
            )}
          />
        </Panel>
        <Panel>
          <ExampleTable title='Table title' />
        </Panel>
      </LayoutMain>
      <LayoutRightSidebar open={logsOpen}>
        <Panel>
          <Header
            variant='title3'
            slot={() => (
              <Button
                variant='normalBorderless'
                onClick={() => {
                  setLogsOpen(false);
                }}
              >
                <Icon name='close' />
              </Button>
            )}
          >
            Logs
          </Header>
          <Label colorName='lowContrast'>A list of all activities.</Label>
          <Button variant='normalBorderless' className='self-start'>
            Download logs
          </Button>
        </Panel>
        <Panel>
          <Row
            title={() => 'Sync succeeded'}
            titleSuffix={() => '18:19'}
            description={() => '12KB'}
            descriptionSuffix={() => '25/8/2023'}
            footnote={() => 'Job id: 2133'}
          />
          <Row
            title={() => 'Sync succeeded'}
            titleSuffix={() => '18:19'}
            description={() => '12KB'}
            descriptionSuffix={() => '25/8/2023'}
            footnote={() => 'Job id: 2133'}
          />
          <Row
            title={() => 'Sync succeeded'}
            titleSuffix={() => '18:19'}
            description={() => '12KB'}
            descriptionSuffix={() => '25/8/2023'}
            footnote={() => 'Job id: 2133'}
          />
          <Row
            title={() => 'Sync succeeded'}
            titleSuffix={() => '18:19'}
            description={() => '12KB'}
            descriptionSuffix={() => '25/8/2023'}
            footnote={() => 'Job id: 2133'}
          />
          <Row
            title={() => 'Sync succeeded'}
            titleSuffix={() => '18:19'}
            description={() => '12KB'}
            descriptionSuffix={() => '25/8/2023'}
            footnote={() => 'Job id: 2133'}
          />
          <Row
            title={() => 'Sync succeeded'}
            titleSuffix={() => '18:19'}
            description={() => '12KB'}
            descriptionSuffix={() => '25/8/2023'}
            footnote={() => 'Job id: 2133'}
          />
          <Row
            title={() => 'Sync succeeded'}
            titleSuffix={() => '18:19'}
            description={() => '12KB'}
            descriptionSuffix={() => '25/8/2023'}
            footnote={() => 'Job id: 2133'}
          />
        </Panel>
      </LayoutRightSidebar>
    </>
  );
}
