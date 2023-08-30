SELECT COUNT(*) FROM pg_test.test.test_table;

SELECT INT8 FROM pg_test.test.test_table;
SELECT -INT8 FROM pg_test.test.test_table;
SELECT (INT8 + 1) FROM pg_test.test.test_table;
SELECT (INT8 - 1) FROM pg_test.test.test_table;
SELECT (INT8 * 65536) FROM pg_test.test.test_table;
SELECT (INT8 / INT8) FROM pg_test.test.test_table;

SELECT BOOL FROM pg_test.test.test_table;
SELECT NOT BOOL FROM pg_test.test.test_table;

SELECT DATE FROM pg_test.test.test_table;
SELECT DATE::TIMESTAMP FROM pg_test.test.test_table;
SELECT DATE + INTERVAL '1 DAY' FROM pg_test.test.test_table;

SELECT TIME FROM pg_test.test.test_table;
SELECT TIME + INTERVAL '-17 SECONDS' FROM pg_test.test.test_table;

SELECT INT2 FROM pg_test.test.test_table;
SELECT (INT2 - 2)::INT4 FROM pg_test.test.test_table;

SELECT INT4 FROM pg_test.test.test_table;
SELECT (INT4 - 1729)::INT8 FROM pg_test.test.test_table;

SELECT FLOAT4 FROM pg_test.test.test_table;
SELECT (FLOAT4 - 1.03)::FLOAT8 FROM pg_test.test.test_table;
SELECT (FLOAT4 - 2)::INT4 FROM pg_test.test.test_table;

SELECT FLOAT8 FROM pg_test.test.test_table;
SELECT (FLOAT8 + 4000000000000)::INT8 FROM pg_test.test.test_table;

SELECT numeric FROM pg_test.test.test_table;
SELECT SUM(numeric) FROM pg_test.test.test_table;
SELECT (numeric - 1.2)::text  FROM pg_test.test.test_table;
SELECT (numeric - 200.2)::float4 FROM pg_test.test.test_table;
SELECT numeric::integer FROM pg_test.test.test_table;
SELECT (numeric + 43423424234242342342432432423432432432432432424200.2)::text FROM pg_test.test.test_table;
SELECT 1000 * numeric FROM pg_test.test.test_table;
SELECT numeric::INT4 - 100 FROM pg_test.test.test_table;

SELECT TEXT FROM pg_test.test.test_table;
SELECT SUBSTRING(TEXT, 5, 6) FROM pg_test.test.test_table;

SELECT TIMESTAMP FROM pg_test.test.test_table;
SELECT TIMESTAMP::TIMESTAMPTZ FROM pg_test.test.test_table;
SELECT (TIMESTAMP + INTERVAL '1 YEAR, 1 MONTH, 1 DAY, 1 SECOND') FROM pg_test.test.test_table;

SELECT TIMESTAMPTZ FROM pg_test.test.test_table;
SELECT TIMESTAMPTZ::TIMESTAMP FROM pg_test.test.test_table;
SELECT (TIMESTAMPTZ - INTERVAL '100 YEARS, 100 MONTHS, 100 DAYS, 100 SECONDS') FROM pg_test.test.test_table;

SELECT BYTEA FROM pg_test.test.test_table;
SELECT LENGTH(BYTEA) FROM pg_test.test.test_table;

SELECT UUID FROM pg_test.test.test_table;

SELECT JSONB FROM pg_test.test.test_table;
SELECT JSONB -> 'age' FROM pg_test.test.test_table;
SELECT JSONB -> 'gender' FROM pg_test.test.test_table;

SELECT INT8 FROM pg_test.test.test_table WHERE INT4=172;

SELECT internet4 FROM pg_test.test.test_table;
SELECT internet6 FROM pg_test.test.test_table;
SELECT internet6::TEXT FROM pg_test.test.test_table;
SELECT cidr4 FROM pg_test.test.test_table;
SELECT cidr6 FROM pg_test.test.test_table;
SELECT netmask(cidr4) FROM pg_test.test.test_table;
SELECT network(cidr4)::TEXT FROM pg_test.test.test_table;
SELECT * FROM pg_test.test.test_table WHERE cidr4 << '192.168.0.0/24'::CIDR;

DROP TABLE pg_test.test.test_table;

CREATE TABLE IF NOT EXISTS pg_test.test.temp_table(INT4 INT4, BOOL BOOL, INT8 INT8 PRIMARY KEY);
INSERT INTO pg_test.test.temp_table VALUES(17, true, 26);
SELECT * FROM pg_test.test.temp_table;
UPDATE pg_test.test.temp_table SET INT4 = INT4 + 7 WHERE INT8 = 24;
SELECT * FROM pg_test.test.temp_table;
UPDATE pg_test.test.temp_table SET INT4 = INT4 + 7 WHERE INT8 = 26;
SELECT * FROM pg_test.test.temp_table;
DELETE FROM pg_test.test.temp_table WHERE INT4 = 26;
SELECT * FROM pg_test.test.temp_table;
DELETE FROM pg_test.test.temp_table WHERE INT4 = 24;
SELECT * FROM pg_test.test.temp_table;
INSERT INTO pg_test.test.temp_table VALUES(1, true, 1);
SELECT * FROM pg_test.test.temp_table;
TRUNCATE TABLE pg_test.test.temp_table;
DROP TABLE pg_test.test.temp_table;