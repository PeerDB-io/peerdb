'use client';
import { RenderObject } from '../types';
import { isDefined } from '../utils/isDefined';
import { renderObjectWith } from '../utils/renderObjectWith';
import {
  RowContainer,
  RowVariant,
  RowWrapper,
  StyledDescription,
  StyledDescriptionSuffix,
  StyledFootnote,
  StyledPreTitle,
  StyledTitle,
  StyledTitleSuffix,
  TextContent,
} from './Row.styles';

type RowProps = {
  variant?: RowVariant;
  leadingIcon?: RenderObject;
  thumbnail?: RenderObject;
  preTitle?: string;
  title?: RenderObject;
  titleSuffix?: string;
  description?: RenderObject;
  descriptionSuffix?: RenderObject;
  footnote?: RenderObject;
  trailingIcon?: RenderObject;
  className?: string;
};

/**
 * Row layout component
 *
 * [Figma spec](https://www.figma.com/file/DBMDh1LNNvp9H99N9lZgJ7/PeerDB?type=design&node-id=1-374&mode=dev)
 */
export function Row({
  leadingIcon,
  thumbnail,
  preTitle,
  title,
  titleSuffix,
  description,
  descriptionSuffix,
  footnote,
  trailingIcon,
  variant = 'default',
  ...wrapperProps
}: RowProps) {
  const Title = isDefined(title) && <StyledTitle>{title}</StyledTitle>;
  const PreTitle = isDefined(preTitle) && (
    <StyledPreTitle>{preTitle}</StyledPreTitle>
  );
  const TitleSuffix = isDefined(titleSuffix) && (
    <StyledTitleSuffix>{titleSuffix}</StyledTitleSuffix>
  );
  const Description = isDefined(description) && (
    <StyledDescription>{description}</StyledDescription>
  );
  const DescriptionSuffix = isDefined(descriptionSuffix) && (
    <StyledDescriptionSuffix>{descriptionSuffix}</StyledDescriptionSuffix>
  );
  const Footnote = isDefined(footnote) && (
    <StyledFootnote>{footnote}</StyledFootnote>
  );

  const LeadingIcon = renderObjectWith(leadingIcon, {
    style: { gridArea: 'leading-icon' },
  });

  const TrailingIcon = renderObjectWith(trailingIcon, {
    style: { gridArea: 'trailing-icon' },
  });

  const Thumbnail = renderObjectWith(thumbnail, {
    style: { gridArea: 'thumbnail' },
  });

  return (
    <RowContainer $variant={variant} {...wrapperProps}>
      <RowWrapper>
        {LeadingIcon}
        {Thumbnail}
        <TextContent>
          {PreTitle}
          <RowWrapper>
            {Title}
            {TitleSuffix}
          </RowWrapper>
          <RowWrapper>
            {Description}
            {DescriptionSuffix}
          </RowWrapper>
          {Footnote}
        </TextContent>
        {TrailingIcon}
      </RowWrapper>
    </RowContainer>
  );
}
