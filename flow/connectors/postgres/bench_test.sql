CREATE SCHEMA IF NOT EXISTS bench;

CREATE TABLE bench.large_table (
    id bigserial PRIMARY KEY,
    text1 text,
    text2 text,
    text3 text,
    text4 text,
    uuid1 uuid,
    uuid2 uuid,
    uuid3 uuid,
    float1 float8,
    float2 float8,
    float3 float8,
    float4 float8,
    int1 int,
    int2 int,
    int3 int,
    int4 int
);

DO
$do$
DECLARE
  counter bigint := 0;
BEGIN
  WHILE counter < 5000000 LOOP -- adjust the number based on your needs
    INSERT INTO bench.large_table(
      text1, text2, text3, text4,
      uuid1, uuid2, uuid3,
      float1, float2, float3, float4,
      int1, int2, int3, int4
    )
    VALUES (
      md5(random()::text),
      md5(random()::text),
      md5(random()::text),
      md5(random()::text),
      gen_random_uuid(),
      gen_random_uuid(),
      gen_random_uuid(),
      random() * 1000000,
      random() * 1000000,
      random() * 1000000,
      random() * 1000000,
      floor(random() * 100000)::int,
      floor(random() * 100000)::int,
      floor(random() * 100000)::int,
      floor(random() * 100000)::int
    );
    counter := counter + 1;

    -- Print progress every 1000 rows
    IF counter % 1000 = 0 THEN
      RAISE NOTICE 'Inserted % rows', counter;
    END IF;
  END LOOP;
END
$do$
;
