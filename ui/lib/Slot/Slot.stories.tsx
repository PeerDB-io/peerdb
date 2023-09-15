import { Meta, StoryObj } from '@storybook/react';
import { Label } from '../Label';
import { Slot } from './Slot';

export default {
  title: 'Components / Slot',
  component: Slot,
  tags: ['autodocs'],
  args: {
    children: <Label>Swap using Instance Swap Property</Label>,
  },
} as Meta<typeof Slot>;

type Story = StoryObj<typeof Slot>;
export const Default: Story = {};
