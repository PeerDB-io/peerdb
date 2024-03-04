import styled from 'styled-components';

export const LayoutWrapper = styled.div`
  display: flex;
  background-color: ${({ theme }) => theme.colors.base.background.normal};
  min-height: 100vh;
  width: 100vw;
`;

export const ContentWrapper = styled.div`
  background-color: ${({ theme }) => theme.colors.base.background.normal};
  padding: ${({ theme }) => theme.spacing.medium};
  height: 100vh;
  width: 100%;
  overflow-y: auto;
`;

export type StyledMainProps = {
  $width?: 'large' | 'xxLarge' | 'full';
  $alignSelf: 'center' | 'flex-start' | 'flex-end';
  $justifySelf: 'center' | 'flex-start' | 'flex-end';
  $topPadding?: boolean;
};
export const StyledMain = styled.main<StyledMainProps>`
  padding-top: ${({ theme, $topPadding = false }) =>
    `${$topPadding ? theme.spacing['6xLarge'] : 0}`};
  padding-bottom: ${({ theme, $topPadding = false }) =>
    `${$topPadding ? theme.spacing['6xLarge'] : 0}`};
  width: ${({ $width = 'full', theme }) =>
    $width === 'full' ? '100%' : theme.size[$width]};
  align-self: ${({ $alignSelf }) => $alignSelf};
  justify-self: ${({ $justifySelf }) => $justifySelf};
`;

export const LayoutRightSidebarWrapper = styled.div`
  position: fixed;
  top: 0;
  right: 0;
  height: 100vh;
  width: ${({ theme }) => theme.size.xLarge};
  overflow-y: auto;

  translate: 100%;
  transition: translate 0.25s linear;
  background-color: ${({ theme }) => theme.colors.base.background.normal};

  &[data-open='true'] {
    translate: 0%;
  }
`;
