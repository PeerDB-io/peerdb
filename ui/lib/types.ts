import type { ReactNode } from 'react';

export type RenderSlot<TProps = void> = (props: TProps) => ReactNode;

type PropsOf<
  E extends keyof JSX.IntrinsicElements | React.JSXElementConstructor<any>,
> = JSX.LibraryManagedAttributes<E, React.ComponentPropsWithRef<E>>;

export interface AsProps<E extends React.ElementType = React.ElementType> {
  as?: E;
  className?: string;
}

export type AsComponentProps<E extends React.ElementType> = AsProps<E> &
  Omit<PropsOf<E>, keyof AsProps>;

export type PolymorphicComponentProps<E extends React.ElementType, P> = P &
  AsComponentProps<E>;
