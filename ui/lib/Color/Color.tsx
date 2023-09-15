'use client';
import {
  Children,
  ComponentProps,
  PropsWithChildren,
  cloneElement,
  isValidElement,
} from 'react';
import { useTheme } from 'styled-components';
import { appTheme } from '../AppTheme/appTheme';

type ColorProps<
  TColorCategory extends keyof (typeof appTheme)['colors'],
  TColorVariant extends keyof (typeof appTheme)['colors'][TColorCategory],
> = PropsWithChildren<{
  /** If this component should inject the style value directly into the child component */
  asChild?: boolean;
  /** The color category */
  colorCategory: TColorCategory;
  /** The color variant, based on chosen `colorCategory` */
  colorVariant: TColorVariant;
  /** The color name, based on chosen `colorCategory` and `colorVariant` */
  colorName: keyof (typeof appTheme)['colors'][TColorCategory][TColorVariant];
}>;
/**
 * Component to wrap an element in order to change its text color.
 *
 * Sets the `color` style value to the chosen color
 */
export function Color<
  TColorCategory extends keyof (typeof appTheme)['colors'],
  TColorVariant extends keyof (typeof appTheme)['colors'][TColorCategory],
>({
  colorCategory,
  colorVariant,
  colorName,
  children,
  asChild = true,
}: ColorProps<TColorCategory, TColorVariant>) {
  const theme = useTheme();
  const colorValue = theme.colors[colorCategory][colorVariant][
    colorName
  ] as string;

  const childElement = Children.only(children);
  if (asChild && isValidElement<ComponentProps<any>>(childElement)) {
    return cloneElement(childElement, {
      style: { color: colorValue },
    });
  }
  return <span style={{ color: colorValue }}>{children}</span>;
}
