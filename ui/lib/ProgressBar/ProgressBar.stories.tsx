import { Meta, StoryObj } from '@storybook/react';
import { ProgressBar } from './ProgressBar';

export default {
  title: 'Components / ProgressBar',
  component: ProgressBar,
  tags: ['autodocs'],
  args: {
    progress: 50,
  },
} satisfies Meta<typeof ProgressBar>;

type Story = StoryObj<typeof ProgressBar>;
export const Default: Story = {};
