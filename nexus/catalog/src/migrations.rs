use anyhow::Context;
use tokio_postgres::Client;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

pub async fn run(client: &mut Client, target_version: Option<i32>) -> anyhow::Result<()> {
    let mut runner = embedded::migrations::runner();
    // Tolerate a schema history that is ahead of the binary to support release rollbacks.
    // Divergent migrations with same version but different checksum will still abort.
    runner = runner.set_abort_missing(false);
    if let Some(version) = target_version {
        runner = runner.set_target(refinery::Target::Version(version));
    }
    let migration_report = runner
        .run_async(client)
        .await
        .context("Failed to run migrations")?;
    for migration in migration_report.applied_migrations() {
        tracing::info!(
            "Migration Applied - Name: {}, Version: {}",
            migration.name(),
            migration.version()
        );
    }
    Ok(())
}
