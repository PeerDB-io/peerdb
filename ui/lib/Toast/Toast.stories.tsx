import { Meta, StoryObj } from '@storybook/react';
import { Icon } from '../Icon';
import { ProgressCircle } from '../ProgressCircle';
import { Toast } from './Toast';

export default {
  title: 'Components / Toast',
  component: Toast,
  tags: ['autodocs'],
  args: {
    message: 'Message',
    actionText: 'Action',
    icon: <Icon name='info' />,
    open: true,
  },
} satisfies Meta<typeof Toast>;

type Story = StoryObj<typeof Toast>;
export const DefaultInteractive: Story = {};

export const DefaultNonInteractive: Story = {
  args: {
    actionText: undefined,
  },
};

export const DefaultTimed: Story = {
  args: {
    icon: <ProgressCircle variant='determinate_progress_circle' />,
  },
};
