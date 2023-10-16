'use client';
import { ComponentProps } from 'react';
import { BaseThumbnail, ThumbnailSize } from './Thumbnail.styles';

type ThumbnailProps = {
  size: ThumbnailSize;
  className?: string;
} & ComponentProps<'img'>;

/**
 * Thumbnail component
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-1532&mode=dev)
 */
export function Thumbnail({ size, ref: _ref, ...imageProps }: ThumbnailProps) {
  return <BaseThumbnail $size={size} {...imageProps} />;
}
