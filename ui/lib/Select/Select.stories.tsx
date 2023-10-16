import { Meta, StoryObj } from '@storybook/react';
import { Separator } from '../Separator';
import { Select } from './Select';
import { SelectItem } from './SelectItem';

export default {
  title: 'Components / Input / Select',
  component: Select,
  tags: ['autodocs'],
} satisfies Meta<typeof Select>;

type Story = StoryObj<typeof Select>;
export const Default: Story = {
  render: () => (
    <Select placeholder='Select' name='storybook-select'>
      <SelectItem value='apple'>Apple</SelectItem>
      <SelectItem value='banana'>Banana</SelectItem>
      <SelectItem value='blueberry'>Blueberry</SelectItem>
      <Separator />
      <SelectItem value='grapes'>Grapes</SelectItem>
      <SelectItem value='pineapple'>Pineapple</SelectItem>
    </Select>
  ),
};

export const Disabled: Story = {
  render: () => (
    <Select placeholder='Select' name='storybook-select' disabled>
      <SelectItem value='apple'>Apple</SelectItem>
      <SelectItem value='banana'>Banana</SelectItem>
      <SelectItem value='blueberry'>Blueberry</SelectItem>
      <SelectItem value='grapes'>Grapes</SelectItem>
      <SelectItem value='pineapple'>Pineapple</SelectItem>
    </Select>
  ),
};
