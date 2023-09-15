import { Meta, StoryObj } from '@storybook/react';
import { Button } from '../Button';
import { Label } from '../Label';
import { Dialog, DialogClose } from './Dialog';

export default {
  title: 'Components / Dialog',
  component: Dialog,
  tags: ['autodocs'],
  args: {
    triggerButton: <Button>Open Dialog</Button>,
    size: 'xxSmall',
  },
  render: (props) => (
    <Dialog {...props}>
      <Label>Dialog content</Label>
      <DialogClose>
        <Button>Close</Button>
      </DialogClose>
    </Dialog>
  ),
} as Meta<typeof Dialog>;

type Story = StoryObj<typeof Dialog>;
export const Default: Story = {
  args: {
    size: 'xxSmall',
  },
};
