//! URL parsing for storage backends.
//!
//! Extracts backend configuration from various URL formats (S3, GCS, Azure, local filesystem).

use object_store::path::Path;
use regex::Regex;
use std::collections::HashMap;
use std::sync::OnceLock;

use crate::error::{InvalidUrlSnafu, StorageError};

use super::{AzureConfig, GcsConfig, LocalConfig, S3Config};

// URL patterns for different storage backends
const S3_PATH: &str =
    r"^https://s3\.(?P<region>[\w\-]+)\.amazonaws\.com/(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
const S3_VIRTUAL: &str =
    r"^https://(?P<bucket>[a-z0-9\-\.]+)\.s3\.(?P<region>[\w\-]+)\.amazonaws\.com(/(?P<key>.+))?$";
const S3_URL: &str = r"^[sS]3[aA]?://(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
const S3_ENDPOINT_URL: &str = r"^[sS]3[aA]?::(?<protocol>https?)://(?P<endpoint>[^:/]+):(?<port>\d+)/(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";

const FILE_URI: &str = r"^file://(?P<path>.*)$";
const FILE_URL: &str = r"^file:(?P<path>.*)$";
const FILE_PATH: &str = r"^/(?P<path>.*)$";

const GCS_VIRTUAL: &str =
    r"^https://(?P<bucket>[a-z0-9\-_\.]+)\.storage\.googleapis\.com(/(?P<key>.+))?$";
const GCS_PATH: &str =
    r"^https://storage\.googleapis\.com/(?P<bucket>[a-z0-9\-_\.]+)(/(?P<key>.+))?$";
const GCS_URL: &str = r"^[gG][sS]://(?P<bucket>[a-z0-9\-\._]+)(/(?P<key>.+))?$";

const ABFS_URL: &str = r"^abfss?://(?P<container>[a-z0-9\-]+)@(?P<account>[a-z0-9]+)\.dfs\.core\.windows\.net(/(?P<key>.+))?$";
const AZURE_HTTPS: &str = r"^https://(?P<account>[a-z0-9]+)\.(blob|dfs)\.core\.windows\.net/(?P<container>[a-z0-9\-]+)(/(?P<key>.+))?$";

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
enum Backend {
    S3,
    Gcs,
    Azure,
    Local,
}

fn matchers() -> &'static HashMap<Backend, Vec<Regex>> {
    static MATCHERS: OnceLock<HashMap<Backend, Vec<Regex>>> = OnceLock::new();
    MATCHERS.get_or_init(|| {
        let mut m = HashMap::new();

        m.insert(
            Backend::S3,
            vec![
                Regex::new(S3_PATH).unwrap(),
                Regex::new(S3_VIRTUAL).unwrap(),
                Regex::new(S3_ENDPOINT_URL).unwrap(),
                Regex::new(S3_URL).unwrap(),
            ],
        );

        m.insert(
            Backend::Gcs,
            vec![
                Regex::new(GCS_PATH).unwrap(),
                Regex::new(GCS_VIRTUAL).unwrap(),
                Regex::new(GCS_URL).unwrap(),
            ],
        );

        m.insert(
            Backend::Azure,
            vec![
                Regex::new(ABFS_URL).unwrap(),
                Regex::new(AZURE_HTTPS).unwrap(),
            ],
        );

        m.insert(
            Backend::Local,
            vec![
                Regex::new(FILE_URI).unwrap(),
                Regex::new(FILE_URL).unwrap(),
                Regex::new(FILE_PATH).unwrap(),
            ],
        );

        m
    })
}

/// Backend configuration enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendConfig {
    S3(S3Config),
    Gcs(GcsConfig),
    Azure(AzureConfig),
    Local(LocalConfig),
}

impl BackendConfig {
    /// Parse a URL into a backend configuration.
    pub fn parse_url(url: &str, with_key: bool) -> Result<Self, StorageError> {
        for (k, v) in matchers() {
            if let Some(matches) = v.iter().filter_map(|r| r.captures(url)).next() {
                return match k {
                    Backend::S3 => Self::parse_s3(&matches),
                    Backend::Gcs => Self::parse_gcs(&matches),
                    Backend::Azure => Self::parse_azure(&matches),
                    Backend::Local => Self::parse_local(&matches, with_key),
                };
            }
        }

        InvalidUrlSnafu {
            url: url.to_string(),
        }
        .fail()
    }

    fn parse_s3(matches: &regex::Captures) -> Result<Self, StorageError> {
        let bucket = matches
            .name("bucket")
            .expect("bucket should always be available")
            .as_str()
            .to_string();

        let region = std::env::var("AWS_DEFAULT_REGION")
            .ok()
            .or_else(|| matches.name("region").map(|m| m.as_str().to_string()));

        let endpoint = std::env::var("AWS_ENDPOINT").ok().or_else(|| {
            matches.name("endpoint").map(|endpoint| {
                let port = matches
                    .name("port")
                    .and_then(|p| p.as_str().parse::<u16>().ok())
                    .unwrap_or(443);
                let protocol = matches
                    .name("protocol")
                    .map(|p| p.as_str())
                    .unwrap_or("https");
                format!("{protocol}://{}:{port}", endpoint.as_str())
            })
        });

        let key = matches.name("key").map(|m| m.as_str().into());

        Ok(BackendConfig::S3(S3Config {
            endpoint,
            region,
            bucket,
            key,
        }))
    }

    fn parse_gcs(matches: &regex::Captures) -> Result<Self, StorageError> {
        let bucket = matches
            .name("bucket")
            .expect("bucket should always be available")
            .as_str()
            .to_string();

        let key = matches.name("key").map(|r| r.as_str().into());

        Ok(BackendConfig::Gcs(GcsConfig { bucket, key }))
    }

    fn parse_azure(matches: &regex::Captures) -> Result<Self, StorageError> {
        let container = matches
            .name("container")
            .expect("container should always be available")
            .as_str()
            .to_string();

        let account = matches
            .name("account")
            .expect("account should always be available")
            .as_str()
            .to_string();

        let key = matches.name("key").map(|r| r.as_str().into());

        Ok(BackendConfig::Azure(AzureConfig {
            account,
            container,
            key,
        }))
    }

    fn parse_local(matches: &regex::Captures, with_key: bool) -> Result<Self, StorageError> {
        let path = matches
            .name("path")
            .expect("path regex must contain a path group")
            .as_str();

        let mut path = if !path.starts_with('/') {
            std::path::PathBuf::from(format!("/{path}"))
        } else {
            std::path::PathBuf::from(path)
        };

        let key = if with_key {
            let key = path
                .file_name()
                .map(|k| k.to_str().unwrap().to_string().into());
            path.pop();
            key
        } else {
            None
        };

        Ok(BackendConfig::Local(LocalConfig {
            path: path.to_str().unwrap().to_string(),
            key,
        }))
    }

    pub(crate) fn key(&self) -> Option<&Path> {
        match self {
            BackendConfig::S3(s3) => s3.key.as_ref(),
            BackendConfig::Gcs(gcs) => gcs.key.as_ref(),
            BackendConfig::Azure(azure) => azure.key.as_ref(),
            BackendConfig::Local(local) => local.key.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_url_parsing() {
        let config = BackendConfig::parse_url("s3://mybucket/path/to/data", false).unwrap();
        match config {
            BackendConfig::S3(s3) => {
                assert_eq!(s3.bucket, "mybucket");
                assert_eq!(s3.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_s3_path_style_url() {
        let config = BackendConfig::parse_url(
            "https://s3.us-east-1.amazonaws.com/mybucket/path/to/data",
            false,
        )
        .unwrap();
        match config {
            BackendConfig::S3(s3) => {
                assert_eq!(s3.bucket, "mybucket");
                assert_eq!(s3.region, Some("us-east-1".to_string()));
                assert_eq!(s3.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_s3_virtual_hosted_url() {
        let config = BackendConfig::parse_url(
            "https://mybucket.s3.us-west-2.amazonaws.com/path/to/data",
            false,
        )
        .unwrap();
        match config {
            BackendConfig::S3(s3) => {
                assert_eq!(s3.bucket, "mybucket");
                assert_eq!(s3.region, Some("us-west-2".to_string()));
                assert_eq!(s3.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_s3_endpoint_url() {
        let config =
            BackendConfig::parse_url("s3::http://localhost:9000/mybucket/path/to/data", false)
                .unwrap();
        match config {
            BackendConfig::S3(s3) => {
                assert_eq!(s3.bucket, "mybucket");
                assert_eq!(s3.endpoint, Some("http://localhost:9000".to_string()));
                assert_eq!(s3.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_gcs_url_parsing() {
        let config = BackendConfig::parse_url("gs://mybucket/path/to/data", false).unwrap();
        match config {
            BackendConfig::Gcs(gcs) => {
                assert_eq!(gcs.bucket, "mybucket");
                assert_eq!(gcs.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected Gcs config"),
        }
    }

    #[test]
    fn test_gcs_path_style_url() {
        let config = BackendConfig::parse_url(
            "https://storage.googleapis.com/mybucket/path/to/data",
            false,
        )
        .unwrap();
        match config {
            BackendConfig::Gcs(gcs) => {
                assert_eq!(gcs.bucket, "mybucket");
                assert_eq!(gcs.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected Gcs config"),
        }
    }

    #[test]
    fn test_gcs_virtual_hosted_url() {
        let config = BackendConfig::parse_url(
            "https://mybucket.storage.googleapis.com/path/to/data",
            false,
        )
        .unwrap();
        match config {
            BackendConfig::Gcs(gcs) => {
                assert_eq!(gcs.bucket, "mybucket");
                assert_eq!(gcs.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected Gcs config"),
        }
    }

    #[test]
    fn test_local_url_parsing() {
        let config = BackendConfig::parse_url("/local/path/to/data", false).unwrap();
        match config {
            BackendConfig::Local(local) => {
                assert_eq!(local.path, "/local/path/to/data");
            }
            _ => panic!("Expected Local config"),
        }
    }

    #[test]
    fn test_local_file_uri() {
        let config = BackendConfig::parse_url("file:///local/path/to/data", false).unwrap();
        match config {
            BackendConfig::Local(local) => {
                assert_eq!(local.path, "/local/path/to/data");
            }
            _ => panic!("Expected Local config"),
        }
    }

    #[test]
    fn test_local_file_url() {
        let config = BackendConfig::parse_url("file:/local/path/to/data", false).unwrap();
        match config {
            BackendConfig::Local(local) => {
                assert_eq!(local.path, "/local/path/to/data");
            }
            _ => panic!("Expected Local config"),
        }
    }

    #[test]
    fn test_azure_url_parsing() {
        let config = BackendConfig::parse_url(
            "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/path/to/data",
            false,
        )
        .unwrap();
        match config {
            BackendConfig::Azure(azure) => {
                assert_eq!(azure.account, "mystorageaccount");
                assert_eq!(azure.container, "mycontainer");
                assert_eq!(azure.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected Azure config"),
        }
    }

    #[test]
    fn test_azure_https_url() {
        let config = BackendConfig::parse_url(
            "https://mystorageaccount.blob.core.windows.net/mycontainer/path/to/data",
            false,
        )
        .unwrap();
        match config {
            BackendConfig::Azure(azure) => {
                assert_eq!(azure.account, "mystorageaccount");
                assert_eq!(azure.container, "mycontainer");
                assert_eq!(azure.key, Some(Path::from("path/to/data")));
            }
            _ => panic!("Expected Azure config"),
        }
    }

    #[test]
    fn test_local_with_key() {
        let config = BackendConfig::parse_url("/local/path/to/file.txt", true).unwrap();
        match config {
            BackendConfig::Local(local) => {
                assert_eq!(local.path, "/local/path/to");
                assert_eq!(local.key, Some(Path::from("file.txt")));
            }
            _ => panic!("Expected Local config"),
        }
    }

    #[test]
    fn test_invalid_url() {
        let result = BackendConfig::parse_url("invalid://url", false);
        assert!(result.is_err());
    }
}
