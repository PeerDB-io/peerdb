import cn from 'classnames';
import { Attributes, cloneElement, isValidElement } from 'react';
import { RenderObject } from '../types';
import { isDefined } from './isDefined';

export const renderObjectWith = <
  TProps extends Partial<TProps> & Attributes & { className?: string },
>(
  renderObject?: RenderObject,
  injectedProps?: TProps
) => {
  if (!isDefined(renderObject)) return null;

  const element = renderObject;
  if (!isValidElement<TProps>(element)) return null;

  const elementClassName =
    'className' in element.props
      ? typeof element.props.className === 'string'
        ? element.props.className
        : undefined
      : undefined;
  const injectedClassName =
    injectedProps && 'className' in injectedProps
      ? typeof injectedProps.className === 'string'
        ? injectedProps.className
        : undefined
      : undefined;

  const className = cn(elementClassName, injectedClassName);

  return cloneElement(element, { ...injectedProps, className } as TProps);
};
