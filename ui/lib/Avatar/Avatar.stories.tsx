import { Meta, StoryObj } from '@storybook/react';
import { Avatar } from './Avatar';
import checkerImage from './checker.png';

export default {
  title: 'Components / Avatar',
  component: Avatar,
  tags: ['autodocs'],
  args: {
    size: 'small',
  },
  argTypes: {
    variant: {
      description: 'The variant of the Avatar',
    },
    size: {
      description: 'The size of the Avatar',
    },
  },
} satisfies Meta<typeof Avatar>;

type Story = StoryObj<typeof Avatar>;
export const Image: Story = {
  args: {
    variant: 'image',
    src: checkerImage.src,
    alt: 'Checker Image Avatar',
  },
};

export const Text: Story = {
  args: {
    variant: 'text',
    text: 'MP',
  },
};

export const Icon: Story = {
  args: {
    variant: 'icon',
    name: 'square',
  },
};
