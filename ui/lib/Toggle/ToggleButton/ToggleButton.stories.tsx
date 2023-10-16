import { Meta, StoryObj } from '@storybook/react';
import { Icon } from '../../Icon';
import { ToggleButton } from './ToggleButton';

export default {
  title: 'Components / Toggle / ToggleButton',
  component: ToggleButton,
  tags: ['autodocs'],
  args: {
    disabled: false,
    pressed: false,
  },
} satisfies Meta<typeof ToggleButton>;

type Story = StoryObj<typeof ToggleButton>;
export const DefaultIcon: Story = {
  args: {
    children: <Icon name='square' />,
  },
};

export const DefaultLabel: Story = {
  args: {
    children: 'Toggle',
  },
};

export const SelectedIcon: Story = {
  args: {
    pressed: true,
    children: <Icon name='square' />,
  },
};

export const SelectedLabel: Story = {
  args: {
    pressed: true,
    children: 'Toggle',
  },
};

export const DisabledIcon: Story = {
  args: {
    disabled: true,
    children: <Icon name='square' />,
  },
};

export const DisabledLabel: Story = {
  args: {
    disabled: true,
    children: 'Toggle',
  },
};
