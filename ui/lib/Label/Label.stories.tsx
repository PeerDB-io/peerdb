import { Meta, StoryObj } from '@storybook/react';
import { Label } from './Label';

export default {
  title: 'Components / Label',
  component: Label,
  tags: ['autodocs'],
  args: {
    children: 'Label',
  },
} as Meta<typeof Label>;

type Story = StoryObj<typeof Label>;
export const Body: Story = {
  args: {
    variant: 'body',
  },
};

export const Action: Story = {
  args: {
    variant: 'action',
  },
};

export const Footnote: Story = {
  args: {
    variant: 'footnote',
  },
};

export const Subheadline: Story = {
  args: {
    variant: 'subheadline',
  },
};

export const Headline: Story = {
  args: {
    variant: 'headline',
  },
};

export const Title3: Story = {
  args: {
    variant: 'title3',
  },
};

export const Title2: Story = {
  args: {
    variant: 'title2',
  },
};

export const Title1: Story = {
  args: {
    variant: 'title1',
  },
};

export const LargeTitle: Story = {
  args: {
    variant: 'largeTitle',
  },
};
