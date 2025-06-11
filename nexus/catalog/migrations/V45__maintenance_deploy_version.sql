ALTER TABLE maintenance.start_maintenance_outputs
    ADD COLUMN IF NOT EXISTS api_deploy_version TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS cli_deploy_version TEXT NOT NULL DEFAULT '';
