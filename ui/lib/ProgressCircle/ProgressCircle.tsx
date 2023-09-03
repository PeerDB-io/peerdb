import { BaseIcon, ProgressCircleVariant } from './ProgressCircle.styles';

type ProgressCircleProps = {
  variant: ProgressCircleVariant;
  className?: string;
};

export function ProgressCircle({ variant, ...iconProps }: ProgressCircleProps) {
  return <BaseIcon {...iconProps} $variant={variant} name={variant} />;
}
