use crate::client::{convert_to_string, join_path};
use crate::config::{RemoteConfig, TargetConfig};
use anyhow::{anyhow, Context, Result};
use bytes::BytesMut;
use chrono::Local;
use itertools::Itertools;
use openssh::{KnownHosts, Session, SessionBuilder};
use openssh_sftp_client::Sftp;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;

pub struct RemoteConfigClient {
    config: RemoteConfig,
    sessions: HashMap<String, Session>,
    sftp_clients: HashMap<String, Sftp>,
}

impl RemoteConfigClient {
    pub async fn new(config: &RemoteConfig) -> Result<Self> {
        let mut sessions = HashMap::new();
        let mut sftp_clients = HashMap::new();

        for server in &config.servers {
            let mut builder = SessionBuilder::default();
            builder.known_hosts_check(KnownHosts::Accept);
            if let Some(identity) = &config.identity {
                builder.keyfile(identity);
            }
            builder.control_directory("/tmp");
            let session = timeout(
                Duration::from_secs(config.timeout.unwrap_or(5)),
                builder.connect(format!("ssh://{}@{}", config.user, server.host)),
            )
            .await
            .map_err(|_| anyhow!("Timeout connect to {}@{}", config.user, server.host))??;
            sessions.insert(server.name(), session);

            let session = timeout(
                Duration::from_secs(config.timeout.unwrap_or(5)),
                builder.connect(format!("ssh://{}@{}", config.user, server.host)),
            )
            .await
            .map_err(|_| anyhow!("Timeout connect to {}@{}", config.user, server.host))??;
            sftp_clients.insert(
                server.name(),
                Sftp::from_session(session, Default::default()).await?,
            );
        }

        let client = RemoteConfigClient {
            config: config.clone(),
            sessions,
            sftp_clients,
        };

        Ok(client)
    }

    fn remote_session(&self, server_name: &str) -> Result<&Session> {
        let session = self
            .sessions
            .get(server_name)
            .with_context(|| format!("Not found connection. (server={})", &server_name))?;
        Ok(session)
    }

    fn sftp_client(&self, server_name: &str) -> Result<&Sftp> {
        let sftp = self
            .sftp_clients
            .get(server_name)
            .with_context(|| format!("Not found connection. (server={})", &server_name))?;
        Ok(sftp)
    }

    async fn remote_command(&self, server_name: &str, command: &str, sudo: bool) -> Result<String> {
        let mut command = command.to_owned();
        if sudo {
            command = format!("sudo sh -c \"{}\"", command);
        }

        let output = self
            .remote_session(server_name)?
            .raw_command(&command)
            .output()
            .await?;

        let stdout = String::from_utf8(output.stdout)?;
        let stderr = String::from_utf8(output.stderr)?;

        if !output.status.success() {
            return Err(anyhow!(
                "Failed execute command\ncommand: {}\nstdout: {}\nstderr: {}",
                command,
                stdout,
                stderr,
            ));
        }

        Ok(stdout)
    }

    pub async fn exists(&self, server_name: &str, target: &TargetConfig) -> Result<bool> {
        let command = format!("ls {}", target.path);
        let exists = self
            .remote_command(server_name, &command, target.sudo)
            .await
            .is_ok();
        Ok(exists)
    }

    pub async fn exists_relative_path(
        &self,
        server_name: &str,
        target: &TargetConfig,
        relative_path: &Path,
    ) -> Result<bool> {
        let path = self.real_path(server_name, target, relative_path)?;
        let command = format!("ls {}", convert_to_string(&path)?);
        let exists = self
            .remote_command(server_name, &command, target.sudo)
            .await
            .is_ok();
        Ok(exists)
    }

    pub async fn file_relative_paths(
        &self,
        server_name: &str,
        target: &TargetConfig,
    ) -> Result<Vec<PathBuf>> {
        if !self.exists(server_name, target).await? {
            return Ok(vec![]);
        }
        let command = format!("find {} -type f -o -type l", target.path);
        let result = self
            .remote_command(server_name, &command, target.sudo)
            .await?;
        let paths: Result<Vec<_>, _> = result
            .split_whitespace()
            .filter_map(|s| {
                if s.is_empty() {
                    return None;
                }
                let home_prefix = format!("/home/{}", self.config.user);
                if target.path.starts_with('~') && s.starts_with(&home_prefix) {
                    return Some(s.replacen(&home_prefix, "~", 1));
                }
                Some(s.to_owned())
            })
            .map(|s| Path::new(&s).to_owned())
            .map(|p| p.strip_prefix(&target.path).map(|path| path.to_owned()))
            .collect();
        Ok(paths?)
    }

    pub fn real_path(
        &self,
        _server_name: &str,
        target: &TargetConfig,
        relative_path: &Path,
    ) -> Result<PathBuf> {
        Ok(join_path(Path::new(&target.path), relative_path))
    }

    pub async fn len(
        &self,
        server_name: &str,
        target: &TargetConfig,
        relative_path: &Path,
    ) -> Result<u64> {
        let path = self.real_path(server_name, target, relative_path)?;
        let mut path = convert_to_string(&path)?;

        if !target.sudo && path.starts_with('~') {
            path = path.replacen('~', format!("/home/{}", self.config.user).as_str(), 1);
        }

        self.remote_command(server_name, &format!("stat -L -c %s {}", path), target.sudo)
            .await?
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow!("Failed to parse stat result."))
    }

    async fn read_remote_path(&self, server_name: &str, path: impl AsRef<Path>) -> Result<Vec<u8>> {
        let sftp = self.sftp_client(server_name)?;
        // これ本当か？？？と思っている
        let mut remote_file = sftp.open(path).await?;
        let metadata = remote_file.metadata().await?;
        let buffer = remote_file
            .read_all(metadata.len().unwrap() as usize, BytesMut::new())
            .await?;
        remote_file.close().await?;
        Ok(buffer.to_vec())
    }

    pub async fn get(
        &self,
        server_name: &str,
        target: &TargetConfig,
        relative_path: &Path,
    ) -> Result<Vec<u8>> {
        let path = self.real_path(server_name, target, relative_path)?;
        let config = if target.sudo {
            let tmp_path = format!("/tmp/{}", Local::now().to_rfc3339());

            self.remote_command(
                server_name,
                &format!("cp {} {}", convert_to_string(&path)?, tmp_path),
                true,
            )
            .await?;

            self.remote_command(server_name, &format!("chmod 644 {}", tmp_path), true)
                .await?;
            let config = self.read_remote_path(server_name, &tmp_path).await?;
            self.remote_command(server_name, &format!("rm {}", tmp_path), true)
                .await?;
            config
        } else {
            let mut path = convert_to_string(&path)?;
            if path.starts_with('~') {
                path = path.replacen('~', format!("/home/{}", self.config.user).as_str(), 1);
            }
            self.read_remote_path(server_name, &path).await?
        };

        Ok(config)
    }

    pub async fn create(
        &self,
        server_name: &str,
        target: &TargetConfig,
        relative_path: &Path,
        config_bytes: Vec<u8>,
    ) -> Result<()> {
        let path = self.real_path(server_name, target, relative_path)?;
        let sftp = self.sftp_client(server_name)?;

        if target.sudo {
            let tmp_path = format!("/tmp/{}", Local::now().to_rfc3339());
            let mut remote_file = sftp.create(&tmp_path).await?;
            remote_file.write_all(&config_bytes).await?;
            remote_file.close().await?;

            if let Some(parent) = path.parent() {
                self.remote_command(
                    server_name,
                    &format!("mkdir -p {}", convert_to_string(parent)?),
                    true,
                )
                .await?;
            }

            self.remote_command(
                server_name,
                &format!("cp {} {}", tmp_path, convert_to_string(&path)?),
                true,
            )
            .await?;

            self.remote_command(server_name, &format!("rm {}", tmp_path), true)
                .await?;
        } else {
            let mut path = convert_to_string(&path)?;
            if path.starts_with('~') {
                path = path.replacen('~', format!("/home/{}", self.config.user).as_str(), 1);
            }
            let path = Path::new(&path);

            if let Some(parent) = path.parent() {
                self.remote_command(
                    server_name,
                    &format!("mkdir -p {}", convert_to_string(parent)?),
                    false,
                )
                .await?;
            }

            let mut remote_file = sftp.create(path).await?;
            remote_file.write_all(&config_bytes).await?;
            remote_file.close().await?;
        }

        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        let servers = self.sessions.keys().cloned().collect_vec();
        for server in servers {
            let session = self
                .sessions
                .remove(&server)
                .with_context(|| format!("Not found session. (server={})", &server))?;
            session.close().await?;
        }
        Ok(())
    }
}
