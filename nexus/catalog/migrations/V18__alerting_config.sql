CREATE TABLE alerting_settings(
    id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    config_name TEXT NOT NULL,
    config_value TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_alerting_settings_config_name ON alerting_settings (config_name);
