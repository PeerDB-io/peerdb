import * as RadixSelect from '@radix-ui/react-select';
import { Meta, StoryObj } from '@storybook/react';
import { SelectItem } from './SelectItem';

export default {
  title: 'Components / Input / Select / SelectItem',
  component: SelectItem,
  tags: ['autodocs'],
  args: {
    value: 'label',
  },
} satisfies Meta<typeof SelectItem>;

type Story = StoryObj<typeof SelectItem>;
export const Default: Story = {
  render: (props) => (
    <RadixSelect.Root open>
      <RadixSelect.SelectContent>
        <SelectItem {...props}>Label</SelectItem>
      </RadixSelect.SelectContent>
    </RadixSelect.Root>
  ),
};

export const Disabled: Story = {
  args: {
    disabled: true,
  },
  render: (props) => (
    <RadixSelect.Root open>
      <RadixSelect.SelectContent>
        <SelectItem {...props}>Label</SelectItem>
      </RadixSelect.SelectContent>
    </RadixSelect.Root>
  ),
};
