'use client';

import { Meta, StoryObj } from '@storybook/react';
import { Icon } from '../Icon';
import { Label } from '../Label';
import { ToggleGroup, ToggleGroupItem } from '../Toggle';
import { RowWithToggleGroup } from './Layout';

export default {
  title: 'Components / Layout / RowWithToggleGroup',
  component: RowWithToggleGroup,
  args: {
    label: <Label>Label</Label>,
    action: (
      <ToggleGroup defaultValue='item1'>
        <ToggleGroupItem value='item1'>
          <Icon name='square' />
        </ToggleGroupItem>
        <ToggleGroupItem value='item2'>
          <Icon name='square' />
        </ToggleGroupItem>
        <ToggleGroupItem value='item3'>
          <Icon name='square' />
        </ToggleGroupItem>
      </ToggleGroup>
    ),
  },
} satisfies Meta<typeof RowWithToggleGroup>;

export const Default: StoryObj<typeof RowWithToggleGroup> = {};
