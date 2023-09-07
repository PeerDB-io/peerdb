import styled from 'styled-components';
import { Label } from '../Label';

export const HeaderWrapper = styled.div`
  display: flex;
`;

export const StyledLabel = styled(Label)`
  flex: 1 1 auto;

  padding: ${({ variant, theme }) =>
    variant === 'largeTitle' ||
    variant === 'title1' ||
    variant === 'title2' ||
    variant === 'title3'
      ? `0 ${theme.spacing.medium}`
      : `${theme.spacing.xSmall} ${theme.spacing.medium}`};
`;
