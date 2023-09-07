import { Meta, StoryObj } from '@storybook/react';
import { Label } from '../Label';
import { Color } from './Color';

export default {
  title: 'Utils / Color',
  component: Color,
  tags: ['autodocs'],
} satisfies Meta;

type Story = StoryObj<typeof Color>;
export const Default: Story = {
  render: () => (
    <Color colorCategory='positive' colorVariant='text' colorName='lowContrast'>
      <Label variant='headline'>Success</Label>
    </Color>
  ),
};
