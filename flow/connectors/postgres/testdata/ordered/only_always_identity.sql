DO $_peerdb_apply$#variable_conflict use_column
<<_peerdb_apply>> DECLARE r record; BEGIN
FOR r IN SELECT x._peerdb_record_type AS t,n."id" AS n0,o."id" AS o0 FROM _peerdb_internal._peerdb_raw_mirror x, jsonb_to_record(x._peerdb_data) n("id" int8),
jsonb_to_record(x._peerdb_data || coalesce(x._peerdb_match_data, '{}')) o("id" int8)
WHERE x._peerdb_batch_id = 7 AND x._peerdb_destination_table_name = 'public.t' ORDER BY x._peerdb_timestamp
LOOP
  IF r.t = 2 THEN
    IF _peerdb_apply.r.o0 IS NULL THEN
      DELETE FROM "public"."t" dst WHERE (dst.tableoid, dst.ctid) = (SELECT x.tableoid, x.ctid FROM "public"."t" x WHERE (_peerdb_apply.r.n0 IS NULL OR x."id"::text = _peerdb_apply.r.n0::text) LIMIT 1);
    ELSE
      DELETE FROM "public"."t" dst WHERE dst."id" = _peerdb_apply.r.o0;
    END IF;
  ELSE
    IF r.t = 1 THEN
      PERFORM FROM "public"."t" dst WHERE dst."id" = _peerdb_apply.r.o0;
    END IF;
    IF r.t = 0 OR NOT FOUND THEN
      INSERT INTO "public"."t" ("id") OVERRIDING SYSTEM VALUE SELECT _peerdb_apply.r.n0 WHERE NOT EXISTS (SELECT FROM "public"."t" x WHERE x."id" = _peerdb_apply.r.n0);
      IF NOT FOUND THEN
        PERFORM FROM "public"."t" dst WHERE dst."id" = _peerdb_apply.r.n0;
      END IF;
    END IF;
  END IF;
END LOOP; END$_peerdb_apply$
