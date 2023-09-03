import { Meta, StoryObj } from '@storybook/react';
import { Icon } from '../Icon';
import { Button } from './Button';

export default {
  title: 'Components / Button',
  component: Button,
  tags: ['autodocs'],
  args: {
    children: 'Action',
    loading: false,
  },
  argTypes: {
    onClick: {
      action: 'Clicked',
    },
  },
} satisfies Meta<typeof Button>;

type Story = StoryObj<typeof Button>;
export const Normal: Story = {
  args: {
    variant: 'normal',
  },
};

export const Destructive: Story = {
  args: {
    variant: 'destructive',
  },
};

export const NormalSolid: Story = {
  args: {
    variant: 'normalSolid',
  },
};

export const DestructiveSolid: Story = {
  args: {
    variant: 'destructiveSolid',
  },
};

export const NormalBorderless: Story = {
  args: {
    variant: 'normalBorderless',
  },
};

export const DestructiveBorderless: Story = {
  args: {
    variant: 'destructiveBorderless',
  },
};

export const WithIcon: Story = {
  args: {
    variant: 'normal',
    children: <Icon name='check_box_outline_blank' />,
  },
};

export const Disabled: Story = {
  args: {
    variant: 'normal',
    disabled: true,
  },
};

export const Loading: Story = {
  args: {
    variant: 'normal',
    loading: true,
  },
};
