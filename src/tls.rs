use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::config::cli::Cli;

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{what}: {source}")]
    Parse {
        what: &'static str,
        source: reqwest::Error,
    },
}

fn read(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::Read {
        path: path.to_owned(),
        source,
    })
}

/// The HTTP client for the inference gateway, with native TLS.
pub fn inference_client(cli: &Cli, max_idle_per_host: usize) -> Result<reqwest::Client, TlsError> {
    let mut builder = reqwest::Client::builder()
        .pool_max_idle_per_host(max_idle_per_host)
        .pool_idle_timeout(Duration::from_secs(90));
    if let Some(ca) = &cli.tls_ca_cert {
        let cert =
            reqwest::Certificate::from_pem(&read(ca)?).map_err(|source| TlsError::Parse {
                what: "CA certificate",
                source,
            })?;
        builder = builder.add_root_certificate(cert);
    }
    if let (Some(cert), Some(key)) = (&cli.tls_cert, &cli.tls_key) {
        let identity =
            reqwest::Identity::from_pkcs8_pem(&read(cert)?, &read(key)?).map_err(|source| {
                TlsError::Parse {
                    what: "client certificate and key",
                    source,
                }
            })?;
        builder = builder.identity(identity);
    }
    if cli.tls_insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    builder.build().map_err(|source| TlsError::Parse {
        what: "HTTP client",
        source,
    })
}
