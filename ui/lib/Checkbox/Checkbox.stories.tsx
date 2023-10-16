import { Meta, StoryObj } from '@storybook/react';
import { Checkbox } from './Checkbox';

export default {
  title: 'Components / Input / Checkbox',
  component: Checkbox,
  tags: ['autodocs'],
  args: {
    name: 'story',
    disabled: false,
    defaultChecked: false,
  },
} satisfies Meta<typeof Checkbox>;

type Story = StoryObj<typeof Checkbox>;
export const Default: Story = {};

export const Mixed: Story = {
  args: {
    checked: true,
    variant: 'mixed',
  },
};

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
