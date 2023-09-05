SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

CREATE SCHEMA IF NOT EXISTS test;

DROP TABLE IF EXISTS test.test_table;

CREATE TABLE test.test_table (
    bool boolean NOT NULL,
    date date NOT NULL,
    "time" time without time zone DEFAULT now() NOT NULL,
    int2 smallint NOT NULL,
    int4 integer DEFAULT 1726 NOT NULL,
    int8 bigint NOT NULL,
    float4 real DEFAULT 3.1415 NOT NULL,
    float8 double precision NOT NULL,
    text text NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    timestamptz timestamp with time zone NOT NULL,
    bytea bytea NOT NULL,
    uuid uuid NOT NULL,
    jsonb jsonb NOT NULL,
    "numeric" numeric,
    internet4 inet,
    internet6 inet,
    cidr4 cidr,
    cidr6 cidr
);

ALTER TABLE test.test_table OWNER TO postgres;

INSERT INTO test.test_table (
    bool, 
    date, 
    "time", 
    int2, 
    int4, 
    int8, 
    float4, 
    float8, 
    text, 
    "timestamp", 
    timestamptz, 
    bytea, 
    uuid, 
    jsonb, 
    "numeric",
    internet4,
    internet6,
    cidr4,
    cidr6
    ) VALUES(
    false,
    '2005-10-10',
    '15:13:33.893341',
    1,
    1726,
    4294967296,
    3.1415,
    2.7182818284,
    'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum',
    '2011-11-11 00:34:56',
    '2222-02-22 15:38:07+00',
    E'\\xdeadbeef',
    '63e8c671-7562-4809-b42a-cd8c2d121b12',	
    '{"age": 36, "guid": "bf8b20d9-c9d7-4d9e-b52d-f6e26dcb1d6e", "name": "Adrian Farley", "email": "adrianfarley@proflex.com", "index": 0, "phone": "+1 (815) 475-2975", "gender": "female", "address": "947 Montana Place, Lacomb, Maine, 4927", "balance": "$1,331.54", "company": "PROFLEX", "picture": "http://placehold.it/32x32", "eyeColor": "blue", "isActive": false, "latitude": -26.125532, "longitude": 174.541577, "registered": "2015-04-06T11:47:48 -06:-30"}',
    123.45,
    '192.168.1.100', 
    '2001:0db8:85a3:0000:0000:8a2e:0370:7334', 
    '192.168.0.0/24', 
    '2001:0db8::/48'
);

ALTER TABLE ONLY test.test_table
    ADD CONSTRAINT test_table_pkey PRIMARY KEY (int8);

GRANT ALL ON TABLE test.test_table TO postgres;
