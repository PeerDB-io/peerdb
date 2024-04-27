const QRepQueryTemplate = `-- Here is a sample template: fill in the table name and watermark column
SELECT * FROM <table_name>
WHERE <watermark_column>
BETWEEN {{.start}} AND {{.end}}`;

export default QRepQueryTemplate;
