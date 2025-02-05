export default function TitleCase(input: string): string {
  if (input == 'BIGQUERY') return 'BigQuery';
  return input
    .toLowerCase()
    .replace(/\b\w/g, function (char) {
      return char.toUpperCase();
    })
    .replaceAll('Postgresql', 'PostgreSQL')
    .replaceAll('Rds', 'RDS')
    .replaceAll('Mysql', 'MySQL');
}
