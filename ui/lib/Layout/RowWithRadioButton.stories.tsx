'use client';

import { Meta, StoryObj } from '@storybook/react';
import { Label } from '../Label';
import { RadioButton, RadioButtonGroup } from '../RadioButtonGroup';
import { RowWithRadiobutton } from './Layout';

export default {
  title: 'Components / Layout / RowWithRadioButton',
  component: RowWithRadiobutton,
  args: {
    label: <Label>Label</Label>,
    action: <RadioButton value='storybook' />,
    description: <Label>Description</Label>,
  },
  render: (props) => (
    <RadioButtonGroup>
      <RowWithRadiobutton {...props} />
    </RadioButtonGroup>
  ),
} satisfies Meta<typeof RowWithRadiobutton>;

export const Default: StoryObj<typeof RowWithRadiobutton> = {};
