CREATE PEER postgres_peer FROM POSTGRES WITH
(
    host = 'sai-test.postgres.database.azure.com',
    port = '5432',
    user = 'postgres',
    password = 'test123!',
    database = 'postgres'
);

-- Query away tables in Postgres
--SELECT * FROM postgres_peer.<schema>.<tablename>;

