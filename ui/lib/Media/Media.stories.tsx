import { Meta, StoryObj } from '@storybook/react';
import { Media } from './Media';
import checkerImage from './checker.png';

export default {
  title: 'Components / Media',
  component: Media,
  tags: ['autodocs'],
  args: {
    src: checkerImage.src,
  },
} as Meta<typeof Media>;

type Story = StoryObj<typeof Media>;
export const AspectRatio1x1: Story = {
  args: {
    ratio: '1 / 1',
  },
};

export const AspectRatio2x1: Story = {
  args: {
    ratio: '2 / 1',
  },
};

export const AspectRatio2x3: Story = {
  args: {
    ratio: '2 / 3',
  },
};

export const AspectRatio3x2: Story = {
  args: {
    ratio: '3 / 2',
  },
};

export const AspectRatio3x4: Story = {
  args: {
    ratio: '3 / 4',
  },
};

export const AspectRatio4x3: Story = {
  args: {
    ratio: '4 / 3',
  },
};

export const AspectRatio4x5: Story = {
  args: {
    ratio: '4 / 5',
  },
};

export const AspectRatio5x4: Story = {
  args: {
    ratio: '5 / 4',
  },
};

export const AspectRatio9x16: Story = {
  args: {
    ratio: '9 / 16',
  },
};

export const AspectRatio16x9: Story = {
  args: {
    ratio: '16 / 9',
  },
};

export const AspectRatio21x9: Story = {
  args: {
    ratio: '21 / 9',
  },
};

export const AspectRatioGolden: Story = {
  args: {
    ratio: 'golden',
  },
};
