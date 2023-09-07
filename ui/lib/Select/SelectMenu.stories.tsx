import * as RadixSelect from '@radix-ui/react-select';
import { Meta, StoryObj } from '@storybook/react';
import { SelectItem } from './SelectItem';
import { SelectMenu } from './SelectMenu';

export default {
  title: 'Components / Input / Select / SelectMenu',
  component: SelectMenu,
  tags: ['autodocs'],
} satisfies Meta<typeof SelectMenu>;

type Story = StoryObj<typeof SelectMenu>;
export const Default: Story = {
  render: (props) => (
    <div style={{ minHeight: '250px' }}>
      <RadixSelect.Root open>
        <SelectMenu {...props}>
          <SelectItem value='item1'>Item one</SelectItem>
          <SelectItem value='item2'>Item two</SelectItem>
          <SelectItem value='item3'>Item three</SelectItem>
          <SelectItem value='item4'>Item four</SelectItem>
          <SelectItem value='item5'>Item five</SelectItem>
          <SelectItem value='item6'>Item six</SelectItem>
        </SelectMenu>
      </RadixSelect.Root>
    </div>
  ),
};
