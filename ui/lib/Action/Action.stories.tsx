import { Meta, StoryObj } from '@storybook/react';
import { Icon } from '../Icon';
import { Action } from './Action';

export default {
  title: 'Components / Action',
  component: Action,
  tags: ['autodocs'],
  args: {
    disabled: false,
    children: 'Label',
    icon: <Icon name='link' />,
    href: '/',
  },
} as Meta<typeof Action>;

type Story = StoryObj<typeof Action>;
export const Default: Story = {};
