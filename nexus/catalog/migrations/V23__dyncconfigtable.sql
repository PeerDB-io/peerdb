ALTER TABLE alerting_settings RENAME TO dynamic_settings;
ALTER TABLE dynamic_settings ADD COLUMN setting_description TEXT;
ALTER TABLE dynamic_settings ADD COLUMN needs_restart BOOLEAN;
