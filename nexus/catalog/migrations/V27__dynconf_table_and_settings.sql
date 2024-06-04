ALTER TABLE alerting_settings RENAME TO dynamic_settings;
ALTER TABLE dynamic_settings ADD COLUMN config_default_value TEXT, ADD COLUMN config_value_type INT, ADD COLUMN config_description TEXT, ADD COLUMN config_apply_mode INT;
ALTER TABLE dynamic_settings ALTER COLUMN config_value DROP NOT NULL;

INSERT INTO dynamic_settings (config_name,config_value,config_default_value,config_value_type,config_description,config_apply_mode)
VALUES
('PEERDB_ALERTING_GAP_MINUTES',null,'15',3,'Duration in minutes before reraising alerts, 0 disables all alerting entirely',1),
('PEERDB_SLOT_LAG_MB_ALERT_THRESHOLD',null,'5000',3,'Lag (in MB) threshold on PeerDB slot to start sending alerts, 0 disables slot lag alerting entirely',1),
('PEERDB_PGPEER_OPEN_CONNECTIONS_ALERT_THRESHOLD',null,'5',3,'Open connections from PeerDB user threshold to start sending alerts, 0 disables open connections alerting entirely',1),
('PEERDB_BIGQUERY_ENABLE_SYNCED_AT_PARTITIONING_BY_DAYS',null,'false',4,'BigQuery only: create target tables with partitioning by _PEERDB_SYNCED_AT column',4);
