// COLUMN_TYPE_PATTERN mirrors structured.ColumnTypeRegex on the server: the column type
// expressions accepted as a column's destination type. Kept in sync by hand.
export const COLUMN_TYPE_PATTERN = /^$|^[a-zA-Z][a-zA-Z0-9(),]*$/;

// StructuredColumn is the part of a ColumnSetting the structured ingestion rules look at.
interface StructuredColumn {
  sourceName: string;
  destinationType: string;
}

// structuredColumnIssues reports why the given columns are not a valid structured ingestion
// schema, mirroring structured.ValidateColumns on the server so that the wizard can flag the
// problem before the mirror is submitted. An empty list means the columns are acceptable.
export function structuredColumnIssues(columns: StructuredColumn[]): string[] {
  if (columns.length === 0) {
    return ['Structured ingestion needs at least one column.'];
  }

  const issues: string[] = [];
  const unnamed = columns.filter((col) => col.sourceName === '').length;
  if (unnamed > 0) {
    issues.push(
      `${unnamed} column${unnamed > 1 ? 's have' : ' has'} no source field.`
    );
  }

  const invalidTypes = columns
    .filter(
      (col) =>
        col.destinationType !== '' &&
        !COLUMN_TYPE_PATTERN.test(col.destinationType)
    )
    .map((col) => col.destinationType);
  if (invalidTypes.length > 0) {
    issues.push(`Invalid destination type: ${invalidTypes.join(', ')}.`);
  }

  // Every column has to declare a destination type: the mapping carries the destination
  // schema itself, so an untyped column has nothing to fall back on.
  const untyped = columns
    .filter((col) => col.destinationType === '')
    .map((col) => col.sourceName || '(unnamed)');
  if (untyped.length > 0) {
    issues.push(`No destination type for: ${untyped.join(', ')}.`);
  }

  return issues;
}
