import { Meta, StoryObj } from '@storybook/react';
import { Label } from '../Label';
import { TextField } from '../TextField';
import { RowWithTextField } from './Layout';

export default {
  title: 'Components / Layout / RowWithTextField',
  component: RowWithTextField,
  args: {
    label: () => <Label>Label</Label>,
    action: () => <TextField variant='simple' placeholder='Placeholder' />,
    description: () => <Label>Description</Label>,
  },
} satisfies Meta<typeof RowWithTextField>;

export const Default: StoryObj<typeof RowWithTextField> = {};

export const WithInstruction: StoryObj<typeof RowWithTextField> = {
  args: {
    instruction: () => <Label>Instruction</Label>,
  },
};
