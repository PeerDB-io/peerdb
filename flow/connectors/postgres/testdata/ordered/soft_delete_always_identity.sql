DO $_peerdb_apply$#variable_conflict use_column
<<_peerdb_apply>> DECLARE r record; BEGIN
FOR r IN SELECT x._peerdb_record_type AS t,n."id" AS n0,o."id" AS o0,n."v" AS n1 FROM _peerdb_internal._peerdb_raw_mirror x, jsonb_to_record(x._peerdb_data) n("id" int8,"v" text),
jsonb_to_record(x._peerdb_data || coalesce(x._peerdb_match_data, '{}')) o("id" int8)
WHERE x._peerdb_batch_id = 7 AND x._peerdb_destination_table_name = 'public.t' ORDER BY x._peerdb_timestamp
LOOP
  IF r.t = 2 THEN
    IF _peerdb_apply.r.o0 IS NULL THEN
      UPDATE "public"."t" dst SET "_PEERDB_IS_DELETED" = TRUE,"_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP WHERE (dst.tableoid, dst.ctid) = (SELECT x.tableoid, x.ctid FROM "public"."t" x WHERE (_peerdb_apply.r.n0 IS NULL OR x."id"::text = _peerdb_apply.r.n0::text) AND (_peerdb_apply.r.n1 IS NULL OR x."v"::text = _peerdb_apply.r.n1::text) LIMIT 1);
      IF NOT FOUND THEN
        INSERT INTO "public"."t" ("id","v","_PEERDB_SYNCED_AT","_PEERDB_IS_DELETED") OVERRIDING SYSTEM VALUE SELECT _peerdb_apply.r.n0,_peerdb_apply.r.n1,CURRENT_TIMESTAMP,TRUE;
      END IF;
    ELSE
      UPDATE "public"."t" dst SET "_PEERDB_IS_DELETED" = TRUE,"_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP WHERE dst."id" = _peerdb_apply.r.o0;
      IF NOT FOUND THEN
        INSERT INTO "public"."t" ("id","v","_PEERDB_SYNCED_AT","_PEERDB_IS_DELETED") OVERRIDING SYSTEM VALUE SELECT _peerdb_apply.r.n0,_peerdb_apply.r.n1,CURRENT_TIMESTAMP,TRUE;
      END IF;
    END IF;
  ELSE
    IF r.t = 1 THEN
      UPDATE "public"."t" dst SET "v" = _peerdb_apply.r.n1,"_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP,"_PEERDB_IS_DELETED" = FALSE WHERE dst."id" = _peerdb_apply.r.o0;
    END IF;
    IF r.t = 0 OR NOT FOUND THEN
      INSERT INTO "public"."t" ("id","v","_PEERDB_SYNCED_AT","_PEERDB_IS_DELETED") OVERRIDING SYSTEM VALUE SELECT _peerdb_apply.r.n0,_peerdb_apply.r.n1,CURRENT_TIMESTAMP,FALSE WHERE NOT EXISTS (SELECT FROM "public"."t" x WHERE x."id" = _peerdb_apply.r.n0);
      IF NOT FOUND THEN
        UPDATE "public"."t" dst SET "v" = _peerdb_apply.r.n1,"_PEERDB_SYNCED_AT" = CURRENT_TIMESTAMP,"_PEERDB_IS_DELETED" = FALSE WHERE dst."id" = _peerdb_apply.r.n0;
      END IF;
    END IF;
  END IF;
END LOOP; END$_peerdb_apply$
