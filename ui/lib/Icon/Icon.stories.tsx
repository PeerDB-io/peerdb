import { Meta, StoryObj } from '@storybook/react';
import { Icon } from './Icon';

export default {
  title: 'Components / Icon',
  component: Icon,
  tags: ['autodocs'],
  args: {
    name: 'square',
    fill: false,
  },
} satisfies Meta<typeof Icon>;

type Story = StoryObj<typeof Icon>;
export const Default: Story = {};

export const Filled: Story = {
  args: {
    fill: true,
  },
};
