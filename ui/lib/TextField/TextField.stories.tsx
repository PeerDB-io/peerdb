import { Meta, StoryObj } from '@storybook/react';
import { TextField } from './TextField';

export default {
  title: 'Components / Input / TextField',
  component: TextField,
  tags: ['autodocs'],
  args: {
    placeholder: 'Placeholder',
    disabled: false,
  },
} satisfies Meta<typeof TextField>;

type Story = StoryObj<typeof TextField>;

export const SimpleField: Story = {
  args: {
    variant: 'simple',
  },
};

export const TextAreaField: Story = {
  args: {
    variant: 'text-area',
  },
};
