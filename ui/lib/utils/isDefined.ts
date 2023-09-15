export const isDefined = <TValue>(
  value: TValue | undefined | null
): value is TValue => value !== null && value !== undefined;
