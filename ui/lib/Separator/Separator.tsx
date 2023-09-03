import { PolymorphicComponentProps } from '../types';
import {
  BaseSeparator,
  SeparatorHeight,
  SeparatorVariant,
} from './Separator.styles';

type SeparatorProps = {
  height?: SeparatorHeight;
  variant?: SeparatorVariant;
};

export function Separator<AsProp extends React.ElementType>({
  height = 'tall',
  variant = 'centered',
  ...separatorProps
}: PolymorphicComponentProps<AsProp, SeparatorProps>) {
  return (
    <BaseSeparator {...separatorProps} $height={height} $variant={variant} />
  );
}
