import { Meta, StoryObj } from '@storybook/react';
import { Button } from '../Button';
import { Label } from '../Label';
import { RowWithButton } from './Layout';

export default {
  title: 'Components / Layout / RowWithButton',
  component: RowWithButton,
  args: {
    label: () => <Label>Label</Label>,
    action: () => <Button>Action</Button>,
    description: () => <Label>Description</Label>,
  },
} satisfies Meta<typeof RowWithButton>;

export const Default: StoryObj<typeof RowWithButton> = {};
