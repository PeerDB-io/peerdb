import styled from 'styled-components';

export type AspectRatio =
  | '1 / 1'
  | '2 / 1'
  | '2 / 3'
  | '3 / 2'
  | '3 / 4'
  | '4 / 3'
  | '4 / 5'
  | '5 / 4'
  | '9 / 16'
  | '16 / 9'
  | '21 / 9'
  | 'golden';

type BaseImageProps = {
  $ratio: AspectRatio;
};

const GOLDEN_RATIO = 1.618033987;

export const BaseImage = styled.img<BaseImageProps>`
  display: flex;
  justify-content: center;
  align-items: center;
  width: 64px;

  border: 0;
  border-radius: ${({ theme }) => theme.radius.xSmall};
  background-color: ${({ theme }) => theme.colors.base.background.normal};

  object-fit: cover;
  object-position: center;

  aspect-ratio: ${({ $ratio }) =>
    $ratio === 'golden' ? GOLDEN_RATIO : $ratio};
`;
