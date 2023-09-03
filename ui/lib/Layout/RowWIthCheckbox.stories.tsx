import { Meta, StoryObj } from '@storybook/react';
import { Checkbox } from '../Checkbox';
import { Label } from '../Label';
import { RowWithCheckbox } from './Layout';

export default {
  title: 'Components / Layout / RowWithCheckbox',
  component: RowWithCheckbox,
  args: {
    label: () => <Label>Label</Label>,
    action: () => <Checkbox />,
    description: () => <Label>Description</Label>,
  },
} satisfies Meta<typeof RowWithCheckbox>;

export const Default: StoryObj<typeof RowWithCheckbox> = {};
