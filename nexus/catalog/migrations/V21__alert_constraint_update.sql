ALTER TABLE peerdb_stats.alerting_config
DROP CONSTRAINT alerting_config_service_type_check;

ALTER TABLE peerdb_stats.alerting_config
ADD CONSTRAINT alerting_config_service_type_check
CHECK (service_type IN ('slack', 'email'));