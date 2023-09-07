import { Meta, StoryObj } from '@storybook/react';
import { Separator } from './Separator';

export default {
  title: 'Components / Separator',
  component: Separator,
  tags: ['autodocs'],
  args: {
    height: 'tall',
    variant: 'normal',
  },
} as Meta<typeof Separator>;

type Story = StoryObj<typeof Separator>;
export const Normal: Story = {
  args: {
    variant: 'normal',
    height: 'tall',
  },
};

export const Indent: Story = {
  args: {
    variant: 'indent',
    height: 'tall',
  },
};

export const Thick: Story = {
  args: {
    variant: 'thick',
    height: 'tall',
  },
};

export const Centered: Story = {
  args: {
    variant: 'centered',
    height: 'tall',
  },
};

export const Empty: Story = {
  args: {
    variant: 'empty',
    height: 'tall',
  },
};
