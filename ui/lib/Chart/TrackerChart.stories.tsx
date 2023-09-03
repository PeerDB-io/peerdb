import { Meta, StoryObj } from '@storybook/react';
import { Color } from '@tremor/react';
import { Label } from '../Label';
import { RowWithToggleGroup } from '../Layout';
import { ToggleGroup, ToggleGroupItem } from '../Toggle';
import { TrackerChart } from './TrackerChart';

const weekdays = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];

interface Tracker {
  color: Color;
  tooltip: string;
}

const weekData = weekdays.map<Tracker>((weekDay) => ({
  tooltip: weekDay,
  color: Math.random() > 0.5 ? 'blue' : 'gray',
}));

export default {
  title: 'Components / Tracker',
  component: TrackerChart,
  tags: ['autodocs'],
  args: {
    data: weekData,
    top: () => (
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
    ),
  },
} as Meta<typeof TrackerChart>;

type Story = StoryObj<typeof TrackerChart>;

export const Default: Story = {};
