import { Meta, StoryObj } from '@storybook/react';
import { SearchField } from './SearchField';

export default {
  title: 'Components / SearchField',
  component: SearchField,
  tags: ['autodocs'],
  args: {
    placeholder: 'Placeholder',
  },
} as Meta<typeof SearchField>;

type Story = StoryObj<typeof SearchField>;
export const Default: Story = {};

export const Disabled: Story = {
  args: {
    disabled: true,
  },
};
