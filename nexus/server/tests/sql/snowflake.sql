-- SELECT distinct
SELECT DISTINCT NAME FROM sf_test.PUBLIC.USERS LIMIT 1;

-- text concat
SELECT DISTINCT NAME || ID::TEXT FROM sf_test.PUBLIC.USERS LIMIT 1;

-- simple additions
SELECT id+1 FROM sf_test.PUBLIC.USERS LIMIT 1;

-- projection aliasing
SELECT id as id_alias FROM sf_test.PUBLIC.USERS LIMIT 1;

-- division and multiplication in projections
SELECT id/5.0,id*5.0 FROM sf_test.PUBLIC.USERS LIMIT 1;

-- typecasting in projections
SELECT id::text FROM sf_test.PUBLIC.USERS LIMIT 1;

-- CASE in projections
SELECT CASE when id=1 then id else 0 end from sf_test.PUBLIC.USERS LIMIT 1;

-- Simple WHERE
SELECT * from sf_test.PUBLIC.USERS where id=1;

-- Multiple WHERE clause
SELECT * from sf_test.PUBLIC.USERS where id=1 AND country='India';

-- Non equality WHERE
SELECT * from sf_test.PUBLIC.USERS where id!=1 LIMIT 1;
SELECT id from sf_test.PUBLIC.USERS where id>1  and id < 5 LIMIT 1;
SELECT id from sf_test.PUBLIC.USERS where id>1  and id < 5 and country !='India' LIMIT 1;
SELECT country from sf_test.PUBLIC.USERS where id>1  and id <= 5 LIMIT 1;

SELECT id from sf_test.PUBLIC.USERS ORDER BY country LIMIT 1;
SELECT id from sf_test.PUBLIC.USERS ORDER BY country DESC LIMIT 1;
SELECT id from sf_test.PUBLIC.USERS ORDER BY id DESC LIMIT 1;
SELECT * from sf_test.PUBLIC.USERS ORDER BY country desc,id asc LIMIT 1;

-- Aggregates
SELECT count(*),sum(id),count(distinct id),avg(id),max(id),min(id) from sf_test.PUBLIC.USERS;

-- group by with aggregates
SELECT country,count(*),sum(id),count(distinct id),avg(id),max(id),min(id) from sf_test.PUBLIC.USERS GROUP BY country;

-- group by with CASE
SELECT country,count(*),sum(CASE WHEN id=1 THEN 1 ELSE 0 END),count(distinct id),avg(id) from sf_test.PUBLIC.USERS GROUP BY country;

-- group by with ORDER BY
SELECT country,count(*) from sf_test.PUBLIC.USERS GROUP BY country ORDER BY 1;
SELECT country,count(*) from sf_test.PUBLIC.USERS GROUP BY country ORDER BY 2;
SELECT country,count(*) from sf_test.PUBLIC.USERS GROUP BY country ORDER BY 2 LIMIT 1;

-- group by, WHERE, ORDER BY, HAVING, LIMIT
SELECT country,count(*),sum(id),count(distinct id),avg(id),max(id),min(id) from sf_test.PUBLIC.USERS where id=1 AND country='India' GROUP BY country HAVING count(*)>0 ORDER BY country DESC LIMIT 1;

-- HAVING
SELECT country,count(*) from sf_test.PUBLIC.USERS GROUP BY country HAVING count(*)>1;

-- explicit join users and events with count
SELECT count(*) from sf_test.PUBLIC.USERS u JOIN sf_test.PUBLIC.EVENTS e ON u.id=e.user_id;

-- non explicit join users and events with count
SELECT count(*) from sf_test.PUBLIC.USERS u,sf_test.PUBLIC.EVENTS e WHERE u.id=e.user_id;

-- Join with GROUP BY, WHERE, ORDER BY, HAVING, LIMIT
SELECT country,count(*) from sf_test.PUBLIC.USERS u JOIN sf_test.PUBLIC.EVENTS e ON u.id=e.user_id where u.id>1 GROUP BY country HAVING count(*)>0 ORDER BY country DESC LIMIT 5;

-- LEFT JOIN
SELECT count(*) from sf_test.PUBLIC.EVENTS e LEFT JOIN sf_test.PUBLIC.USERS u ON u.id=e.user_id WHERE country='Indonesia';

-- RIGHT JOIN
SELECT count(*) from sf_test.PUBLIC.EVENTS e RIGHT JOIN sf_test.PUBLIC.USERS u ON u.id=e.user_id WHERE country='Indonesia';
SELECT count(*) from sf_test.PUBLIC.EVENTS e RIGHT JOIN sf_test.PUBLIC.USERS u ON u.id=e.user_id JOIN sf_test.PUBLIC.USERS u1 ON u1.id=u.id WHERE u1.country='Indonesia';

-- Basic VARIANT: checking if type can be retrieved.
SELECT T FROM sf_test.T_VARIANT LIMIT 1;
SELECT T -> country FROM sf_test.T_VARIANT LIMIT 1;
SELECT T -> country->code FROM sf_test.T_VARIANT LIMIT 1;

-- DATETIME data type tests
SELECT COUNT(*) FROM sf_test.PUBLIC.DATETIME;

-- DATE tests
SELECT DATE FROM sf_test.PUBLIC.DATETIME;
SELECT DATE + INTERVAL '1 DAY' FROM sf_test.PUBLIC.DATETIME;
SELECT DATE + INTERVAL '1 SECOND' FROM sf_test.PUBLIC.DATETIME LIMIT 1;
SELECT DATE + INTERVAL '1000 YEARS, 1000 MONTHS, 100 DAYS, 10 HOURS, 1 MINUTE' FROM sf_test.PUBLIC.DATETIME LIMIT 1;
SELECT DATE + INTERVAL '-1000 YEARS, -1000 MONTHS, -100 DAYS, -10 HOURS, -1 MINUTE' FROM sf_test.PUBLIC.DATETIME WHERE DATE < '2022-01-01'::DATE LIMIT 1;
SELECT DATE + INTERVAL '-1 YEAR, 1 DAY' FROM sf_test.PUBLIC.DATETIME WHERE DATE < '2022-01-01'::DATE LIMIT 1;
SELECT DATE::TIMESTAMP FROM sf_test.PUBLIC.DATETIME LIMIT 1;
SELECT DATE::TIMESTAMP WITH TIME ZONE FROM sf_test.PUBLIC.DATETIME LIMIT 1;

-- TIME tests
SELECT TIME FROM sf_test.PUBLIC.DATETIME;
SELECT TIME + INTERVAL '1 MINUTE' FROM sf_test.PUBLIC.DATETIME LIMIT 1;
SELECT TIME + INTERVAL '1 HOUR' FROM sf_test.PUBLIC.DATETIME WHERE TIME > '22:00'::TIME LIMIT 1;
SELECT TIME + INTERVAL '-23 HOURS, 1 SECOND' FROM sf_test.PUBLIC.DATETIME WHERE TIME > '22:00'::TIME LIMIT 1;
SELECT TIME + INTERVAL '-0.25 SECONDS' FROM sf_test.PUBLIC.DATETIME LIMIT 1;
SELECT TIME + INTERVAL '1000000000000 SECONDS' FROM sf_test.PUBLIC.DATETIME LIMIT 1;
SELECT TIME + INTERVAL '-1000000000000 SECONDS' FROM sf_test.PUBLIC.DATETIME WHERE TIME > '2200'::TIME LIMIT 1;

--TIMESTAMP WITHOUT TIME ZONE tests
SELECT TIMESTAMP_NTZ FROM sf_test.PUBLIC.DATETIME;
SELECT TIMESTAMP_NTZ + INTERVAL '-9 HOURS, -1 SECOND' FROM sf_test.PUBLIC.DATETIME LIMIT 1;
SELECT TIMESTAMP_NTZ + INTERVAL '1 MINUTE' FROM sf_test.PUBLIC.DATETIME WHERE TIMESTAMP_NTZ < '2022-01-01' LIMIT 1;
SELECT TIMESTAMP_NTZ + INTERVAL '1 DAY' FROM sf_test.PUBLIC.DATETIME WHERE TIMESTAMP_NTZ > '2022-01-01' LIMIT 1;
SELECT TIMESTAMP_NTZ + INTERVAL '60 MINUTES, 60 SECONDS' FROM sf_test.PUBLIC.DATETIME LIMIT 1;
SELECT TIMESTAMP_NTZ AT TIME ZONE 'UTC' FROM sf_test.PUBLIC.DATETIME LIMIT 1;
SELECT TIMESTAMP_NTZ AT TIME ZONE 'Asia/Kolkata' FROM sf_test.PUBLIC.DATETIME WHERE TIMESTAMP_NTZ > '2022-01-01' LIMIT 1;

--TIMESTAMP WITH TIME ZONE tests
SELECT TIMESTAMP_TZ FROM sf_test.PUBLIC.DATETIME;
SELECT TIMESTAMP_TZ + INTERVAL '1 DAY' FROM sf_test.PUBLIC.DATETIME WHERE TIMESTAMP_TZ > '2023-01-01' LIMIT 1;
SELECT TIMESTAMP_TZ + INTERVAL '-3 HOURS, -1 SECOND' FROM sf_test.PUBLIC.DATETIME LIMIT 1;
SELECT TIMESTAMP_TZ AT TIME ZONE 'UTC' FROM sf_test.PUBLIC.DATETIME LIMIT 1;
SELECT TIMESTAMP_TZ AT TIME ZONE 'Asia/Kolkata' FROM sf_test.PUBLIC.DATETIME WHERE TIMESTAMP_TZ > '2023-01-01' LIMIT 1;


DECLARE liahona CURSOR FOR SELECT NAME FROM sf_test.PUBLIC.USERS LIMIT 1000;
FETCH 1 IN liahona;
FETCH FORWARD 1 IN liahona;