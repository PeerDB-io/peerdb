'use client';
import { ComponentProps } from 'react';
import { IconProps } from '../Icon';
import {
  AvatarIconBase,
  AvatarImageBase,
  AvatarSize,
  AvatarTextBase,
} from './Avatar.styles';

type AvatarImageProps = {
  variant: 'image';
} & ComponentProps<'img'>;

type AvatarTextProps = {
  variant: 'text';
  text: string;
};

type AvatarIconProps = {
  variant: 'icon';
} & IconProps;

type AvatarProps = { className?: string; size: AvatarSize } & (
  | AvatarImageProps
  | AvatarIconProps
  | AvatarTextProps
);

const isAvatarImage = (
  props: AvatarImageProps | AvatarIconProps | AvatarTextProps
): props is AvatarImageProps => props.variant === 'image';
const isAvatarIcon = (
  props: AvatarImageProps | AvatarIconProps | AvatarTextProps
): props is AvatarIconProps => props.variant === 'icon';

/**
 * Avatar component
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-1499&mode=dev)
 */
export function Avatar({ size, ...baseProps }: AvatarProps) {
  if (isAvatarImage(baseProps)) {
    const { variant: _variant, ref: _ref, ...imageProps } = baseProps;
    return <AvatarImageBase $size={size} {...imageProps} />;
  }
  if (isAvatarIcon(baseProps)) {
    const { variant: _variant, ...iconProps } = baseProps;
    return <AvatarIconBase $size={size} {...iconProps} />;
  }

  const { variant: _variant, text, ...textProps } = baseProps;
  return (
    <AvatarTextBase $size={size} {...textProps}>
      {text}
    </AvatarTextBase>
  );
}
