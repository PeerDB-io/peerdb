'use client';
import { PropsWithChildren } from 'react';
import { BadgeType, BadgeVariant, BadgeWrapper } from './Badge.styles';

type BadgeProps = PropsWithChildren<{
  variant?: BadgeVariant;
  type?: BadgeType;
  className?: string;
}>;
export function Badge({
  variant = 'normal',
  type = 'default',
  ...wrapperProps
}: BadgeProps) {
  return <BadgeWrapper {...wrapperProps} $variant={variant} $type={type} />;
}
