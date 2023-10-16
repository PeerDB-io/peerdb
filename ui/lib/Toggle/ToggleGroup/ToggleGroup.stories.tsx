import { Meta, StoryObj } from '@storybook/react';
import { Icon } from '../../Icon';
import { ToggleGroup } from './ToggleGroup';
import { ToggleGroupItem } from './ToggleGroup.styles';

export default {
  title: 'Components / Toggle / ToggleGroup',
  component: ToggleGroup,
  tags: ['autodocs'],
  args: {
    disabled: false,
  },
  render: (props) => (
    <ToggleGroup {...props} defaultValue='item1'>
      <ToggleGroupItem value='item1'>
        <Icon name='square' />
      </ToggleGroupItem>
      <ToggleGroupItem value='item2'>
        <Icon name='square' />
      </ToggleGroupItem>
      <ToggleGroupItem value='item3'>
        <Icon name='square' />
      </ToggleGroupItem>
    </ToggleGroup>
  ),
} as Meta<typeof ToggleGroup>;

type Story = StoryObj<typeof ToggleGroup>;
export const Default: Story = {};

export const Disabled: Story = {
  args: {
    disabled: true,
  },
};
