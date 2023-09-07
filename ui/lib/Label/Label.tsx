'use client';

import { PolymorphicComponentProps } from '../types';
import {
  BaseLabel,
  LabelColorName,
  LabelColorSet,
  LabelVariant,
} from './Label.styles';

export type LabelProps = {
  /** The variant to render */
  variant?: LabelVariant;

  className?: string;

  colorSet?: LabelColorSet;

  colorName?: LabelColorName;
};
/**
 * Label text component
 *
 * Ensure to set the `as` prop to render the semantically correct html element.
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-797&mode=design&t=9iulGHxuTu4LG7O2-4)
 */
export function Label<AsTarget extends React.ElementType>({
  variant = 'body',
  colorSet = 'base',
  colorName = 'highContrast',
  ...labelProps
}: PolymorphicComponentProps<AsTarget, LabelProps>) {
  return (
    <BaseLabel
      $colorSet={colorSet}
      $colorName={colorName}
      $variant={variant}
      {...labelProps}
    />
  );
}
