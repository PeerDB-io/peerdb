import styled, { css } from 'styled-components';

type WrapperProps = {
  direction: 'row' | 'column';
};

const baseStyles = css<WrapperProps>`
  .layout-label {
    flex: 1 1 50%;
    ${({ direction }) => direction === 'row' && `padding-left: 0px`}
  }
  .layout-description {
    color: ${({ theme }) => theme.colors.base.text.lowContrast};
    ${({ direction }) => direction === 'row' && `padding-left: 0px`}
  }
  .layout-instruction {
    color: ${({ theme }) => theme.colors.destructive.text.lowContrast};
    ${({ direction }) => direction === 'row' && `padding-left: 0px`}
  }
  .layout-suffix {
    color: ${({ theme }) => theme.colors.base.text.lowContrast};
    flex: 0 0 0%;
  }
  .layout-action.layout-action--flex {
    flex: 1 1 50%;
  }
  .layout-action.layout-action--flex-auto {
    flex: 1 1 auto;
  }
  .layout-action-slot {
    flex: 0 0 0%;
  }
`;

export const LayoutWrapper = styled.div<WrapperProps>`
  display: flex;
  flex-direction: ${({ direction }) => direction};
  ${baseStyles}
`;

export const StyledFlexRow = styled.div`
  display: flex;
  flex-flow: row nowrap;
  flex: 1 1 auto;
`;

export const StyledFlexColumn = styled.div`
  display: flex;
  flex-flow: column nowrap;
  flex: 1 1 auto;
`;
