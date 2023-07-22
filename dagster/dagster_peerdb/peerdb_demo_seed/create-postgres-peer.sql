CREATE PEER postgres_peer FROM POSTGRES WITH
(
    host = 'docker.for.mac.localhost',
    port = '5432',
    user = 'postgres',
    password = 'postgres',
    database = 'postgres'
);
