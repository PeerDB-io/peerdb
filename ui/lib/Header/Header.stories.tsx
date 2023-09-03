import { Meta, StoryObj } from '@storybook/react';
import { Header } from './Header';

export default {
  title: 'Components / Header',
  component: Header,
  tags: ['autodocs'],
  args: {
    variant: 'body',
  },
} satisfies Meta<typeof Header>;

type Story = StoryObj<typeof Header>;
export const Body: Story = {
  args: {
    variant: 'body',
    children: 'Body',
  },
};

export const Subheadline: Story = {
  args: {
    variant: 'subheadline',
    children: 'Subheadline',
  },
};

export const Headline: Story = {
  args: {
    variant: 'headline',
    children: 'Headline',
  },
};

export const Title3: Story = {
  args: {
    variant: 'title3',
    children: 'Title 3',
    as: 'h3',
  },
};

export const Title2: Story = {
  args: {
    variant: 'title2',
    children: 'Title 2',
    as: 'h2',
  },
};

export const Title1: Story = {
  args: {
    variant: 'title1',
    children: 'Title 1',
    as: 'h1',
  },
};

export const LargeTitle: Story = {
  args: {
    variant: 'largeTitle',
    children: 'Large Title',
    as: 'h3',
  },
};
