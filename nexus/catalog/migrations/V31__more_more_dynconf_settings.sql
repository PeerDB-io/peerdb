INSERT INTO dynamic_settings (config_name,config_default_value,config_value_type,config_description,config_apply_mode)
VALUES
('PEERDB_MAX_SYNCS_PER_CDC_FLOW','32',3,'Experimental setting: changes number of syncs per workflow, affects frequency of replication slot disconnects',1)
 ON CONFLICT DO NOTHING;
