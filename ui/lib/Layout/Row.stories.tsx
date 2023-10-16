'use client';

import { Meta, StoryObj } from '@storybook/react';
import { Color } from '../Color';
import { Icon } from '../Icon';
import { Thumbnail } from '../Thumbnail';
import { Row } from './Row';
import checkerImage from './checker.png';

export default {
  title: 'Components / Layout / Row',
  component: Row,
  tags: ['autodocs'],
  args: {
    leadingIcon: <Icon name='square' />,
    thumbnail: <Thumbnail size='small' src={checkerImage.src} />,
    trailingIcon: <Icon name='square' />,
    preTitle: 'Pre-title',
    title: (
      <>
        <Color
          colorCategory='positive'
          colorVariant='text'
          colorName='lowContrast'
        >
          <span>Green </span>
        </Color>
        Title
      </>
    ),
    description: 'Description',
    titleSuffix: 'Suffix',
    descriptionSuffix: 'Suffix',
    footnote: 'Footnote',
  },
} satisfies Meta<typeof Row>;

type Story = StoryObj<typeof Row>;
export const Default: Story = {};

export const Focused: Story = {
  args: {
    variant: 'focused',
  },
};

export const Disabled: Story = {
  args: {
    variant: 'disabled',
  },
};
