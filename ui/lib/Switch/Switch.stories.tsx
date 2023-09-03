import { Meta, StoryObj } from '@storybook/react';
import { Switch } from './Switch';

export default {
  title: 'Components / Input / Switch',
  component: Switch,
  tags: ['autodocs'],
  args: {
    name: 'story',
  },
} satisfies Meta<typeof Switch>;

type Story = StoryObj<typeof Switch>;
export const Default: Story = {};

export const Disabled: Story = {
  args: {
    disabled: true,
  },
};

export const Checked: Story = {
  args: {
    checked: true,
  },
};
