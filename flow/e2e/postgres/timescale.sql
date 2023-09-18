CREATE PEER source_pg_2 FROM POSTGRES WITH
(
    host = 'kevin-test-cluster.ctwiqpycdrx0.us-east-2.rds.amazonaws.com',
    port = '5432',
    user = 'postgres',
    password = 'SUMM3RN!GHTS',
    database = 'ts2'
);

CREATE PEER target_ts_2 FROM POSTGRES WITH
(
    host = '3.19.228.194',
    port = '5432',
    user = 'postgres',
    password = 'T1mesc@l3',
    database = 'dst2'
);

CREATE TABLE public.diagnostics (
    id bigint,
    "time" timestamp with time zone,
    tags_id integer,
    fuel_state double precision,
    current_load double precision,
    status double precision,
    additional_tags jsonb, 
    primary key(id, "time")
);

SELECT create_hypertable('diagnostics', 'time', chunk_time_interval => INTERVAL '12 hours');

CREATE TABLE public.readings (
    id bigint,
    "time" timestamp with time zone,
    tags_id integer,
    latitude double precision,
    longitude double precision,
    elevation double precision,
    velocity double precision,
    heading double precision,
    grade double precision,
    fuel_consumption double precision,
    additional_tags jsonb, 
    primary key(id, "time")
);

SELECT create_hypertable('readings', 'time', chunk_time_interval => INTERVAL '12 hours');

CREATE MIRROR tstsv4 FROM source_pg_2 TO target_ts_2 WITH TABLE MAPPING(public.diagnostics:public.diagnostics,public.readings:public.readings);

flow_worker1          | time="2023-08-30T06:47:18Z" level=info msg="RelationMessage => RelationID: 16747, Namespace: public, RelationName: fss1, Columns: [0x400175e360 0x400175e380]"
flow_worker1          | time="2023-08-30T06:47:18Z" level=info msg="23 1 id -1\n"
flow_worker1          | time="2023-08-30T06:47:18Z" level=info msg="20 0 c1 -1\n"
