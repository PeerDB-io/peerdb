import { Meta, StoryObj } from '@storybook/react';
import { ProgressCircle } from './ProgressCircle';

export default {
  title: 'Components / ProgressCircle',
  component: ProgressCircle,
  tags: ['autodocs'],
} satisfies Meta<typeof ProgressCircle>;

type Story = StoryObj<typeof ProgressCircle>;
export const DeterminateProgressCircle: Story = {
  args: {
    variant: 'determinate_progress_circle',
  },
};

export const IndeterminateProgressCircle: Story = {
  args: {
    variant: 'intermediate_progress_circle',
  },
};
