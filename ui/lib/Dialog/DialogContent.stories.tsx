import * as RadixDialog from '@radix-ui/react-dialog';
import { Meta, StoryObj } from '@storybook/react';
import { Label } from '../Label';
import { DialogContent } from './DialogContent';

export default {
  title: 'Components / Dialog / DialogContent',
  component: DialogContent,
  tags: ['autodocs'],
  render: (props) => (
    <RadixDialog.Root open>
      <DialogContent {...props}>
        <Label>Dialog content goes here</Label>
      </DialogContent>
    </RadixDialog.Root>
  ),
} satisfies Meta<typeof DialogContent>;

type Story = StoryObj<typeof DialogContent>;
export const XXSmall: Story = {
  args: {
    size: 'xxSmall',
  },
};

export const XSmall: Story = {
  args: {
    size: 'xSmall',
  },
};

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
