DO $_peerdb_apply$#variable_conflict use_column
<<_peerdb_apply>> DECLARE r record; BEGIN
FOR r IN SELECT x._peerdb_record_type AS t,n."id" AS n0,o."id" AS o0,n."c" AS n1,n."big" AS n2,x._peerdb_data ? 'big' AS p2 FROM _peerdb_internal._peerdb_raw_mirror x, jsonb_to_record(x._peerdb_data) n("id" int4,"c" int4,"big" text),
jsonb_to_record(x._peerdb_data || coalesce(x._peerdb_match_data, '{}')) o("id" int4)
WHERE x._peerdb_batch_id = 7 AND x._peerdb_destination_table_name = 'public.t' ORDER BY x._peerdb_timestamp
LOOP
  IF r.t = 2 THEN
    IF _peerdb_apply.r.o0 IS NULL THEN
      DELETE FROM "public"."t" dst WHERE (dst.tableoid, dst.ctid) = (SELECT x.tableoid, x.ctid FROM "public"."t" x WHERE (_peerdb_apply.r.n0 IS NULL OR x."id"::text = _peerdb_apply.r.n0::text) AND (_peerdb_apply.r.n1 IS NULL OR x."c"::text = _peerdb_apply.r.n1::text) AND (_peerdb_apply.r.n2 IS NULL OR x."big"::text = _peerdb_apply.r.n2::text) LIMIT 1);
    ELSE
      DELETE FROM "public"."t" dst WHERE dst."id" = _peerdb_apply.r.o0;
    END IF;
  ELSE
    IF r.t = 1 THEN
      UPDATE "public"."t" dst SET "id" = _peerdb_apply.r.n0,"c" = _peerdb_apply.r.n1,"big" = CASE WHEN _peerdb_apply.r.p2 THEN _peerdb_apply.r.n2 ELSE dst."big" END WHERE dst."id" = _peerdb_apply.r.o0;
    END IF;
    IF r.t = 0 OR NOT FOUND THEN
      INSERT INTO "public"."t" ("id","c","big") OVERRIDING SYSTEM VALUE SELECT _peerdb_apply.r.n0,_peerdb_apply.r.n1,_peerdb_apply.r.n2 WHERE NOT EXISTS (SELECT FROM "public"."t" x WHERE x."id" = _peerdb_apply.r.n0);
      IF NOT FOUND THEN
        UPDATE "public"."t" dst SET "id" = _peerdb_apply.r.n0,"c" = _peerdb_apply.r.n1,"big" = CASE WHEN _peerdb_apply.r.p2 THEN _peerdb_apply.r.n2 ELSE dst."big" END WHERE dst."id" = _peerdb_apply.r.n0;
      END IF;
    END IF;
  END IF;
END LOOP; END$_peerdb_apply$
