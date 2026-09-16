#[cfg(not(feature = "server"))]
mod migration_cli;
#[cfg(feature = "server")]
#[path = "main.rs"]
mod server;

fn main() -> anyhow::Result<()> {
    #[cfg(feature = "server")]
    return server::main();
    #[cfg(not(feature = "server"))]
    return migration_cli::main();
}
