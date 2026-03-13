// Double-dot escape syntax for source table identifiers.
// Dots within schema/table names are escaped as "..",
// and a lone "." separates schema from table.

export function deparseTableIdentifier(schema: string, table: string): string {
  return schema.replaceAll('.', '..') + '.' + table.replaceAll('.', '..');
}

export function parseTableIdentifier(identifier: string): {
  schema: string;
  table: string;
} {
  // Walk the string looking for the separator: a dot not adjacent to another dot.
  let sep = -1;
  for (let i = 0; i < identifier.length; i++) {
    if (identifier[i] !== '.') continue;
    if (i + 1 < identifier.length && identifier[i + 1] === '.') {
      i++; // skip escaped dot pair
      continue;
    }
    if (sep !== -1) {
      throw new Error(`Invalid table identifier: ${identifier}`);
    }
    sep = i;
  }
  if (sep <= 0 || sep >= identifier.length - 1) {
    throw new Error(`Invalid table identifier: ${identifier}`);
  }
  return {
    schema: identifier.slice(0, sep).replaceAll('..', '.'),
    table: identifier.slice(sep + 1).replaceAll('..', '.'),
  };
}
