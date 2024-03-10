function TitleCase(input: string): string {
  return input
    .toLowerCase()
    .replace(/(?:^|\s)\S/g, function (char) {
      return char.toUpperCase();
    })
    .replace(/Postgresql/g, 'PostgreSQL')
    .replace(/Postgresql/g, 'PostgreSQL')
    .replace(/Rds/g, 'RDS');
}

export default TitleCase;
