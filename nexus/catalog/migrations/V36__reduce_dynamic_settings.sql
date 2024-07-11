delete from dynamic_settings where config_value is null;
alter table dynamic_settings
  alter column config_value set not null,
  drop column config_default_value,
  drop column config_value_type,
  drop column config_description,
  drop column config_apply_mode;
