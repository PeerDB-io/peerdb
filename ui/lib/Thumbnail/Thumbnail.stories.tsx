import { Meta, StoryObj } from '@storybook/react';
import { Thumbnail } from '.';
import checkerImage from './checker.png';

export default {
  title: 'Components / Thumbnail',
  component: Thumbnail,
  tags: ['autodocs'],
  args: {
    src: checkerImage.src,
  },
} as Meta<typeof Thumbnail>;

type Story = StoryObj<typeof Thumbnail>;
export const Small: Story = {
  args: {
    size: 'small',
  },
};

export const Medium: Story = {
  args: {
    size: 'medium',
  },
};

export const Large: Story = {
  args: {
    size: 'large',
  },
};

export const XLarge: Story = {
  args: {
    size: 'xLarge',
  },
};
