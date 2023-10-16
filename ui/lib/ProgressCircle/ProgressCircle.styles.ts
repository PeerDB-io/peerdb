import styled, {
  FlattenSimpleInterpolation,
  css,
  keyframes,
} from 'styled-components';
import { Icon, IconProps } from '../Icon';

const spin = keyframes`
    from {
        rotate: 0deg;
    }
    to {
        rotate: 360deg;
    }
`;

const spin45degIncrements = css`
  animation-name: ${spin};
  animation-duration: 1s;
  animation-direction: normal;
  animation-timing-function: steps(8, end);
  animation-iteration-count: infinite;
`;

const spinLinear = css`
  animation-name: ${spin};
  animation-duration: 1s;
  animation-direction: normal;
  animation-timing-function: linear;
  animation-iteration-count: infinite;
`;

export type ProgressCircleVariant = Extract<
  IconProps['name'],
  'determinate_progress_circle' | 'intermediate_progress_circle'
>;

const variants = {
  determinate_progress_circle: spinLinear,
  intermediate_progress_circle: spin45degIncrements,
} satisfies Record<ProgressCircleVariant, FlattenSimpleInterpolation>;

type BaseIconProps = {
  $variant: ProgressCircleVariant;
};
export const BaseIcon = styled(Icon)<BaseIconProps>`
  ${({ $variant }) => variants[$variant]}
`;
