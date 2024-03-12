SELECT distinct name from bq_test.users LIMIT 1;

-- text concat
SELECT distinct name || id::text from bq_test.users LIMIT 1;

-- simple additions
SELECT id+1 FROM bq_test.users LIMIT 1;

-- projection aliasing
SELECT id as id_alias FROM bq_test.users LIMIT 1;

-- division and multiplication in projections
SELECT id/5.0,id*5.0 FROM bq_test.users LIMIT 1;

-- typecasting in projections
SELECT id::text FROM bq_test.users LIMIT 1;

-- CASE in projections
SELECT CASE when id=1 then id else 0 end from bq_test.users LIMIT 1;

-- Simple WHERE
SELECT * from bq_test.users where id=1;

-- Multiple WHERE clause
SELECT * from bq_test.users where id=1 AND country='India';

-- Non equality WHERE
SELECT * from bq_test.users where id!=1 LIMIT 1;
SELECT id from bq_test.users where id>1  and id < 5 LIMIT 1;
SELECT id from bq_test.users where id>1  and id < 5 and country !='India' LIMIT 1;
SELECT country from bq_test.users where id>1  and id <= 5 LIMIT 1;

-- ORDER BY
SELECT id from bq_test.users ORDER BY country LIMIT 1;
SELECT id from bq_test.users ORDER BY country DESC LIMIT 1;
SELECT id from bq_test.users ORDER BY id DESC LIMIT 1;
SELECT * from bq_test.users ORDER BY country desc,id asc LIMIT 1;


-- Aggregates
SELECT count(*),sum(id),count(distinct id),avg(id),max(id),min(id) from bq_test.users;

-- group by with aggregates
SELECT country,count(*),sum(id),count(distinct id),avg(id),max(id),min(id) from bq_test.users GROUP BY country;

-- group by with CASE
SELECT country,count(*),sum(CASE WHEN id=1 THEN 1 ELSE 0 END),count(distinct id),avg(id) from bq_test.users GROUP BY country;

-- group by with ORDER BY
SELECT country,count(*) from bq_test.users GROUP BY country ORDER BY 1;
SELECT country,count(*) from bq_test.users GROUP BY country ORDER BY 2;
SELECT country,count(*) from bq_test.users GROUP BY country ORDER BY 2 LIMIT 1;

-- group by, WHERE, ORDER BY, HAVING, LIMIT
SELECT country,count(*),sum(id),count(distinct id),avg(id),max(id),min(id) from bq_test.users where id=1 AND country='India' GROUP BY country HAVING count(*)>0 ORDER BY country DESC LIMIT 1;

-- HAVING
SELECT country,count(*) from bq_test.users GROUP BY country HAVING count(*)>1;

-- Joins
-- explicit join users and events with count
SELECT count(*) from bq_test.users u JOIN bq_test.events e ON u.id=e.user_id;

-- non explicit join users and events with count
SELECT count(*) from bq_test.users u,bq_test.events e WHERE u.id=e.user_id;

-- Join with GROUP BY, WHERE, ORDER BY, HAVING, LIMIT
SELECT country,count(*) from bq_test.users u JOIN bq_test.events e ON u.id=e.user_id where u.id>1 GROUP BY country HAVING count(*)>0 ORDER BY country DESC LIMIT 5;

-- LEFT JOIN
SELECT count(*) from bq_test.events e LEFT JOIN bq_test.users u ON u.id=e.user_id WHERE country='Indonesia';

-- RIGHT JOIN
SELECT count(*) from bq_test.events e RIGHT JOIN bq_test.users u ON u.id=e.user_id WHERE country='Indonesia';

SELECT count(*) from bq_test.events e RIGHT JOIN bq_test.users u ON u.id=e.user_id JOIN bq_test.users u1 ON u1.id=u.id WHERE u1.country='Indonesia';

SELECT * FROM bq_test.test_types;

SELECT * from bq_test.Test_Case JOIN bq_test.users ON i=id;

-- ANY eq to IN
SELECT user_id, os FROM bq_test.events WHERE ((id = ANY ('{100,150}'::integer[]))) ORDER BY id;
SELECT user_id, os FROM bq_test.events WHERE ((os = ANY ('{macos,ios}'::text[]))) ORDER BY id LIMIT 2;

-- fixing issues with unsupported BIGINT type for array flattening and cast
SELECT * FROM bq_test.events WHERE id IN (1,2,3);
SELECT * FROM bq_test.events WHERE id = ANY(CAST('{1,2,3}' AS BIGINT[]));
SELECT * FROM bq_test.events WHERE id = ANY(CAST('{1,2,3}' AS BIGINT[])) LIMIT 1;

-- fixing issues with ARRAY with WHERE
SELECT * FROM bq_test.events WHERE id = ANY(CAST(ARRAY[1] AS BIGINT[]));
SELECT * FROM bq_test.events WHERE id = ANY(ARRAY[1]);

SELECT * FROM bq_test.events WHERE id = ANY(CAST('{1}' AS BIGINT[]));
SELECT * FROM bq_test.events WHERE os = ANY(ARRAY['mac']::text[]);
SELECT * FROM bq_test.events WHERE os = ANY(CAST(ARRAY['mac'] as text[]));

-- array arguments in functions
SELECT i FROM bq_test.test_array WHERE (test_dataset.array_intersect(i, '{1,2}'::integer[]));
SELECT i FROM bq_test.test_array_string WHERE (test_dataset.array_intersect(i, '{sai}'::text[]));

-- UNION (by itself) to UNION DISTINCT
SELECT * FROM bq_test.transactions UNION SELECT * FROM bq_test.transactions;
SELECT * FROM bq_test.transactions UNION DISTINCT SELECT * FROM bq_test.transactions;

-- Array agg
SELECT array_agg(c1) FROM bq_test.test_types LIMIT 1;
SELECT array_agg(c2) FROM bq_test.test_types LIMIT 1;
SELECT array_agg(c3) FROM bq_test.test_types LIMIT 1;
SELECT array_agg(c13) FROM bq_test.test_types LIMIT 1;
SELECT array_agg(c20) FROM bq_test.test_types LIMIT 1;
SELECT array_agg(chain) FROM bq_test.transactions;

SELECT count(*) FROM (bq_test.users r1 INNER JOIN bq_test.events r2 ON (((r2.user_id = 1)) AND ((r1.id = 1))));
SELECT NULL FROM bq_test.transactions;

SELECT COALESCE(chain,'sai'::text) FROM bq_test.transactions;
SELECT date_trunc('month',tx_timestamp),count(*) FROM bq_test.transactions GROUP BY 1;

-- NUMERIC, BIGNUMERIC columns
SELECT c19,c18 FROM bq_test.test_types LIMIT 1;
SELECT c19 - 1233434.5 FROM bq_test.test_types LIMIT 1;
SELECT c18 + 20100000000 FROM bq_test.test_types LIMIT 1;
SELECT sum(c19), max(c18) FROM bq_test.test_types LIMIT 1;