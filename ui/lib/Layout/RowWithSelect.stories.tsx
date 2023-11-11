import { Meta, StoryObj } from '@storybook/react';
import { Label } from '../Label';
import { Select, SelectItem } from '../Select';
import { RowWithSelect } from './Layout';

export default {
  title: 'Components / Layout / RowWithSelect',
  component: RowWithSelect,
  args: {
    label: (
      <Label as='label' htmlFor='storybook-select'>
        Label
      </Label>
    ),
    action: <div>select</div>,
    description: <Label>Description</Label>,
  },
} satisfies Meta<typeof RowWithSelect>;

export const Default: StoryObj<typeof RowWithSelect> = {};
