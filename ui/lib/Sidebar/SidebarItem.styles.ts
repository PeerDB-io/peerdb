import { styled } from 'styled-components';
import { Label } from '../Label';

export const StyledLabel = styled(Label)`
  padding: 0;
  flex: 1 1 auto;
  color: var(
    --text-color,
    ${({ theme }) => theme.colors.base.text.highContrast}
  );
`;

export const StyledSuffix = styled(Label)`
  padding: 0;
  color: var(
    --text-color-suffix,
    ${({ theme }) => theme.colors.base.text.lowContrast}
  );
`;

export const BaseItem = styled.button`
  all: unset;

  display: flex;
  flex-flow: row nowrap;
  padding: ${({ theme }) => `${theme.spacing.xxSmall} ${theme.spacing.medium}`};
  margin: ${({ theme }) => theme.spacing.xxSmall} 0px;
  column-gap: ${({ theme }) => theme.spacing.medium};
  align-items: center;
  border-radius: ${({ theme }) => theme.radius.medium};

  cursor: pointer;

  .sidebar-item-icon--leading {
    color: var(
      --text-color,
      ${({ theme }) => theme.colors.base.text.lowContrast}
    );
  }
  .sidebar-item-icon--trailing {
    color: var(
      --text-color,
      ${({ theme }) => theme.colors.base.text.lowContrast}
    );
  }

  &[data-selected] {
    --text-color: ${({ theme }) => theme.colors.special.fixed.white};
    --text-color-suffix: ${({ theme }) => theme.colors.special.fixed.white};
    background-color: ${({ theme }) => theme.colors.accent.fill.normal};
  }

  &:hover:not([data-selected]) {
    background-color: ${({ theme }) => theme.colors.base.surface.hovered};
  }

  &[data-disabled] {
    opacity: 0.3;
    pointer-events: none;
  }

  &:focus-visible:not([data-disabled]) {
    outline: 2px solid ${({ theme }) => theme.colors.accent.border.normal};
    outline-offset: -2px;
    .sidebar-item-icon--trailing {
      --text-color: ${({ theme }) => theme.colors.base.text.highContrast};
    }
  }

  &:focus {
    outline: none;
  }
`;
