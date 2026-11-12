DO $_peerdb_apply$#variable_conflict use_column
<<_peerdb_apply>> DECLARE r record; BEGIN
FOR r IN SELECT x._peerdb_record_type AS t,n."id" AS n0,o."id" AS o0,n."v" AS n1,o."v" AS o1,(n."j" #>> '{}')::jsonb AS n2,(o."j" #>> '{}')::jsonb AS o2 FROM _peerdb_internal._peerdb_raw_mirror x, jsonb_to_record(x._peerdb_data) n("id" int4,"v" text,"j" jsonb),
jsonb_to_record(x._peerdb_data || coalesce(x._peerdb_match_data, '{}')) o("id" int4,"v" text,"j" jsonb)
WHERE x._peerdb_batch_id = 7 AND x._peerdb_destination_table_name = 'public.t' ORDER BY x._peerdb_timestamp
LOOP
  IF r.t = 2 THEN
    DELETE FROM "public"."t" dst WHERE (dst.tableoid, dst.ctid) = (SELECT x.tableoid, x.ctid FROM "public"."t" x WHERE x."id"::text IS NOT DISTINCT FROM _peerdb_apply.r.o0::text AND x."v"::text IS NOT DISTINCT FROM _peerdb_apply.r.o1::text AND x."j"::text IS NOT DISTINCT FROM _peerdb_apply.r.o2::text LIMIT 1);
  ELSE
    IF r.t = 1 THEN
      UPDATE "public"."t" dst SET "id" = _peerdb_apply.r.n0,"v" = _peerdb_apply.r.n1,"j" = _peerdb_apply.r.n2 WHERE (dst.tableoid, dst.ctid) = (SELECT x.tableoid, x.ctid FROM "public"."t" x WHERE x."id"::text IS NOT DISTINCT FROM _peerdb_apply.r.o0::text AND x."v"::text IS NOT DISTINCT FROM _peerdb_apply.r.o1::text AND x."j"::text IS NOT DISTINCT FROM _peerdb_apply.r.o2::text LIMIT 1);
    END IF;
    IF r.t = 0 OR NOT FOUND THEN
      INSERT INTO "public"."t" ("id","v","j") OVERRIDING SYSTEM VALUE SELECT _peerdb_apply.r.n0,_peerdb_apply.r.n1,_peerdb_apply.r.n2;
    END IF;
  END IF;
END LOOP; END$_peerdb_apply$
