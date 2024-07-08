alter table peerdb_stats.alerting_config
  alter column service_config type bytea using convert_to(service_config::text, 'utf-8'),
  add column enc_key_id text not null default '';

