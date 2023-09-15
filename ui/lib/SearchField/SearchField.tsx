'use client';
import { ComponentProps } from 'react';
import { Icon } from '../Icon';
import { BaseInput, StyledWrapper } from './SearchField.styles';

type SearchFieldProps = {} & ComponentProps<typeof BaseInput>;

export function SearchField({ className, ...inputProps }: SearchFieldProps) {
  return (
    <StyledWrapper
      data-disabled={inputProps.disabled || undefined}
      className={className}
    >
      <Icon name='search' />
      <BaseInput {...inputProps} />
    </StyledWrapper>
  );
}
