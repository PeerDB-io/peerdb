ALTER TABLE alerting_settings RENAME TO dynamic_settings;
ALTER TABLE dynamic_settings ADD COLUMN config_value_type INT, ADD COLUMN config_description TEXT, ADD COLUMN config_apply_mode INT, ADD COLUMN config_default_value TEXT;
