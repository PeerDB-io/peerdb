use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum PgError {
    #[snafu(display("Internal error: {}", err_msg))]
    Internal { err_msg: String },
}
