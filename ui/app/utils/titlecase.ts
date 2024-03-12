function TitleCase(input: string): string {
  return input
    .toLowerCase()
    .replace(/\b\w/g, function (char) {
      return char.toUpperCase();
    })
    .replaceAll('Postgresql', 'PostgreSQL')
    .replaceAll('Rds', 'RDS');
}

export default TitleCase;
