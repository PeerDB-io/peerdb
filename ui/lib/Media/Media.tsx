import { ComponentProps } from 'react';
import { AspectRatio, BaseImage } from './Media.styles';

type MediaProps = ComponentProps<'img'> & {
  ratio: AspectRatio;
  className?: string;
};
/**
 * Media component
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-1872&mode=dev)
 */
export function Media({ ratio, ...imageProps }: MediaProps) {
  return <BaseImage $ratio={ratio} {...imageProps} />;
}
