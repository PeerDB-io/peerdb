import { Meta, StoryObj } from '@storybook/react';
import { Button } from '../Button';
import { ButtonGroup } from './ButtonGroup';

export default {
  title: 'Components / ButtonGroup',
  component: ButtonGroup,
  tags: ['autodocs'],
} satisfies Meta<typeof ButtonGroup>;

type Story = StoryObj<typeof ButtonGroup>;

export const OneButton: Story = {
  render: () => (
    <ButtonGroup>
      <Button variant='normalSolid'>Action</Button>
    </ButtonGroup>
  ),
};

export const TwoButtons: Story = {
  render: () => (
    <ButtonGroup>
      <Button variant='normal'>Cancel</Button>
      <Button variant='normalSolid'>Action</Button>
    </ButtonGroup>
  ),
};

export const ThreeButtons: Story = {
  render: () => (
    <ButtonGroup>
      <Button variant='normal'>Cancel</Button>
      <Button variant='normal'>Action</Button>
      <Button variant='normalSolid'>Action</Button>
    </ButtonGroup>
  ),
};
