// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for working with Kafka's client API.

use fancy_regex::Regex;
use std::collections::{btree_map, BTreeMap};
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{bail, Context};
use crossbeam::channel::{unbounded, Receiver, Sender};
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ssh_util::tunnel::{SshTunnelConfig, SshTunnelStatus};
use mz_ssh_util::tunnel_manager::{ManagedSshTunnelHandle, SshTunnelManager};
use rdkafka::client::{BrokerAddr, Client, NativeClient, OAuthToken};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::producer::{DefaultProducerContext, DeliveryResult, ProducerContext};
use rdkafka::types::RDKafkaRespErr;
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Statistics, TopicPartitionList};
use tokio::runtime::Handle;
use tracing::{debug, error, info, warn, Level};

/// A reasonable default timeout when fetching metadata or partitions.
pub const DEFAULT_FETCH_METADATA_TIMEOUT: Duration = Duration::from_secs(30);

/// A `ClientContext` implementation that uses `tracing` instead of `log`
/// macros.
///
/// All code in Materialize that constructs Kafka clients should use this
/// context or a custom context that delegates the `log` and `error` methods to
/// this implementation.
#[derive(Clone)]
pub struct MzClientContext {
    /// The last observed error log, if any.
    error_tx: Sender<MzKafkaError>,
}

impl Default for MzClientContext {
    fn default() -> Self {
        Self::with_errors().0
    }
}

impl MzClientContext {
    /// Constructs a new client context and returns an mpsc `Receiver` that can be used to learn
    /// about librdkafka errors.
    // `crossbeam` channel receivers can be cloned, but this is intended to be used as a mpsc,
    // until we upgrade to `1.72` and the std mpsc sender is `Sync`.
    pub fn with_errors() -> (Self, Receiver<MzKafkaError>) {
        let (error_tx, error_rx) = unbounded();
        (Self { error_tx }, error_rx)
    }

    fn record_error(&self, msg: &str) {
        let err = match MzKafkaError::from_str(msg) {
            Ok(err) => err,
            Err(()) => {
                warn!(original_error = msg, "failed to parse kafka error");
                MzKafkaError::Internal(msg.to_owned())
            }
        };
        // If no one cares about errors we drop them on the floor
        let _ = self.error_tx.send(err);
    }
}

/// A structured error type for errors reported by librdkafka through its logs.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MzKafkaError {
    /// Invalid username or password
    #[error("Invalid username or password")]
    InvalidCredentials,
    /// Missing CA certificate
    #[error("Invalid CA certificate")]
    InvalidCACertificate,
    /// Broker does not support SSL connections
    #[error("Broker does not support SSL connections")]
    SSLUnsupported,
    /// Broker did not provide a certificate
    #[error("Broker did not provide a certificate")]
    BrokerCertificateMissing,
    /// Failed to verify broker certificate
    #[error("Failed to verify broker certificate")]
    InvalidBrokerCertificate,
    /// Connection reset
    #[error("Connection reset: {0}")]
    ConnectionReset(String),
    /// Connection timeout
    #[error("Connection timeout")]
    ConnectionTimeout,
    /// Failed to resolve hostname
    #[error("Failed to resolve hostname")]
    HostnameResolutionFailed,
    /// Unsupported SASL mechanism
    #[error("Unsupported SASL mechanism")]
    UnsupportedSASLMechanism,
    /// Unsupported broker version
    #[error("Unsupported broker version")]
    UnsupportedBrokerVersion,
    /// Connection to broker failed
    #[error("Broker transport failure")]
    BrokerTransportFailure,
    /// All brokers down
    #[error("All brokers down")]
    AllBrokersDown,
    /// SASL authentication required
    #[error("SASL authentication required")]
    SASLAuthenticationRequired,
    /// SSL authentication required
    #[error("SSL authentication required")]
    SSLAuthenticationRequired,
    /// Unknown topic or partition
    #[error("Unknown topic or partition")]
    UnknownTopicOrPartition,
    /// An internal kafka error
    #[error("Internal kafka error: {0}")]
    Internal(String),
}

impl FromStr for MzKafkaError {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains("Authentication failed: Invalid username or password") {
            Ok(Self::InvalidCredentials)
        } else if s.contains("broker certificate could not be verified") {
            Ok(Self::InvalidCACertificate)
        } else if s.contains("client SSL authentication might be required") {
            Ok(Self::SSLAuthenticationRequired)
        } else if s.contains("connecting to a PLAINTEXT broker listener") {
            Ok(Self::SSLUnsupported)
        } else if s.contains("Broker did not provide a certificate") {
            Ok(Self::BrokerCertificateMissing)
        } else if s.contains("Failed to verify broker certificate: ") {
            Ok(Self::InvalidBrokerCertificate)
        } else if let Some((_prefix, inner)) = s.split_once("Send failed: ") {
            Ok(Self::ConnectionReset(inner.to_owned()))
        } else if let Some((_prefix, inner)) = s.split_once("Receive failed: ") {
            Ok(Self::ConnectionReset(inner.to_owned()))
        } else if s.contains("request(s) timed out: disconnect") {
            Ok(Self::ConnectionTimeout)
        } else if s.contains("Failed to resolve") {
            Ok(Self::HostnameResolutionFailed)
        } else if s.contains("mechanism handshake failed:") {
            Ok(Self::UnsupportedSASLMechanism)
        } else if s.contains(
            "verify that security.protocol is correctly configured, \
            broker might require SASL authentication",
        ) {
            Ok(Self::SASLAuthenticationRequired)
        } else if s
            .contains("incorrect security.protocol configuration (connecting to a SSL listener?)")
        {
            Ok(Self::SSLAuthenticationRequired)
        } else if s.contains("probably due to broker version < 0.10") {
            Ok(Self::UnsupportedBrokerVersion)
        } else if s.contains("Disconnected while requesting ApiVersion")
            || s.contains("Broker transport failure")
        {
            Ok(Self::BrokerTransportFailure)
        } else if Regex::new(r"(\d+)/\1 brokers are down")
            .unwrap()
            .is_match(s)
            .unwrap_or_default()
        {
            Ok(Self::AllBrokersDown)
        } else if s.contains("Unknown topic or partition") || s.contains("Unknown partition") {
            Ok(Self::UnknownTopicOrPartition)
        } else {
            Err(())
        }
    }
}

impl ClientContext for MzClientContext {
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        use rdkafka::config::RDKafkaLogLevel::*;
        // Copied from https://docs.rs/rdkafka/0.28.0/src/rdkafka/client.rs.html#58-79
        // but using `tracing`
        match level {
            Emerg | Alert | Critical | Error => {
                self.record_error(log_message);
                // We downgrade error messages to `warn!` level to avoid
                // sending the errors to Sentry. Most errors are customer
                // configuration problems that are not appropriate to send to
                // Sentry.
                warn!(target: "librdkafka", "error: {} {}", fac, log_message);
            }
            Warning => warn!(target: "librdkafka", "warning: {} {}", fac, log_message),
            Notice => info!(target: "librdkafka", "{} {}", fac, log_message),
            Info => info!(target: "librdkafka", "{} {}", fac, log_message),
            Debug => debug!(target: "librdkafka", "{} {}", fac, log_message),
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.record_error(reason);
        // Refer to the comment in the `log` callback.
        warn!(target: "librdkafka", "error: {}: {}", error, reason);
    }
}

impl ConsumerContext for MzClientContext {}

impl ProducerContext for MzClientContext {
    type DeliveryOpaque = <DefaultProducerContext as ProducerContext>::DeliveryOpaque;
    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        DefaultProducerContext.delivery(delivery_result, delivery_opaque);
    }
}

/// Rewrites a broker address.
///
/// For use with [`TunnelingClientContext`].
#[derive(Debug, Clone)]
pub struct BrokerRewrite {
    /// The rewritten hostname.
    pub host: String,
    /// The rewritten port.
    ///
    /// If unspecified, the broker's original port is left unchanged.
    pub port: Option<u16>,
}

#[derive(Clone)]
enum BrokerRewriteHandle {
    Simple(BrokerRewrite),
    SshTunnel(
        // This ensures the ssh tunnel is not shutdown.
        ManagedSshTunnelHandle,
    ),
    /// For _default_ ssh tunnels, we store an error if _creation_
    /// of the tunnel failed, so that `tunnel_status` can return it.
    FailedDefaultSshTunnel(String),
}

/// A client context that supports rewriting broker addresses.
#[derive(Clone)]
pub struct TunnelingClientContext<C> {
    inner: C,
    rewrites: Arc<Mutex<BTreeMap<BrokerAddr, BrokerRewriteHandle>>>,
    default_tunnel: Option<SshTunnelConfig>,
    ssh_tunnel_manager: SshTunnelManager,
    runtime: Handle,
}

impl<C> TunnelingClientContext<C> {
    /// Constructs a new context that wraps `inner`.
    pub fn new(
        inner: C,
        runtime: Handle,
        ssh_tunnel_manager: SshTunnelManager,
    ) -> TunnelingClientContext<C> {
        TunnelingClientContext {
            inner,
            rewrites: Arc::new(Mutex::new(BTreeMap::new())),
            default_tunnel: None,
            ssh_tunnel_manager,
            runtime,
        }
    }

    /// Adds the default broker rewrite rule.
    ///
    /// Connections to brokers that aren't specified in other rewrites will be rewritten to connect to
    /// `rewrite_host` and `rewrite_port` instead.
    pub fn set_default_ssh_tunnel(&mut self, tunnel: SshTunnelConfig) {
        self.default_tunnel = Some(tunnel);
    }

    /// Adds an SSH tunnel for a specific broker.
    ///
    /// Overrides the existing SSH tunnel or rewrite for this broker, if any.
    ///
    /// This tunnel allows the rewrite to evolve over time, for example, if
    /// the ssh tunnel's address changes if it fails and restarts.
    pub async fn add_ssh_tunnel(
        &self,
        broker: BrokerAddr,
        tunnel: SshTunnelConfig,
    ) -> Result<(), anyhow::Error> {
        let ssh_tunnel = self
            .ssh_tunnel_manager
            .connect(
                tunnel,
                &broker.host,
                broker.port.parse().context("parsing broker port")?,
            )
            .await
            .context("creating ssh tunnel")?;

        let mut rewrites = self.rewrites.lock().expect("poisoned");
        rewrites.insert(broker, BrokerRewriteHandle::SshTunnel(ssh_tunnel));
        Ok(())
    }

    /// Adds a broker rewrite rule.
    ///
    /// Overrides the existing SSH tunnel or rewrite for this broker, if any.
    ///
    /// `rewrite` is `BrokerRewrite` that specifies how to rewrite the address for `broker`.
    pub fn add_broker_rewrite(&self, broker: BrokerAddr, rewrite: BrokerRewrite) {
        let mut rewrites = self.rewrites.lock().expect("poisoned");
        rewrites.insert(broker, BrokerRewriteHandle::Simple(rewrite));
    }

    /// Returns a reference to the wrapped context.
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Returns a _consolidated_ `SshTunnelStatus` that communicates the status
    /// of all active ssh tunnels `self` knows about.
    pub fn tunnel_status(&self) -> SshTunnelStatus {
        self.rewrites
            .lock()
            .expect("poisoned")
            .values()
            .map(|handle| match handle {
                BrokerRewriteHandle::SshTunnel(s) => s.check_status(),
                BrokerRewriteHandle::FailedDefaultSshTunnel(e) => {
                    SshTunnelStatus::Errored(e.clone())
                }
                BrokerRewriteHandle::Simple(_) => SshTunnelStatus::Running,
            })
            .fold(SshTunnelStatus::Running, |acc, status| {
                match (acc, status) {
                    (SshTunnelStatus::Running, SshTunnelStatus::Errored(e))
                    | (SshTunnelStatus::Errored(e), SshTunnelStatus::Running) => {
                        SshTunnelStatus::Errored(e)
                    }
                    (SshTunnelStatus::Errored(err), SshTunnelStatus::Errored(e)) => {
                        SshTunnelStatus::Errored(format!("{}, {}", err, e))
                    }
                    (SshTunnelStatus::Running, SshTunnelStatus::Running) => {
                        SshTunnelStatus::Running
                    }
                }
            })
    }
}

impl<C> ClientContext for TunnelingClientContext<C>
where
    C: ClientContext,
{
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = C::ENABLE_REFRESH_OAUTH_TOKEN;

    fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        let return_rewrite = |rewrite: &BrokerRewriteHandle| -> BrokerAddr {
            let rewrite = match rewrite {
                BrokerRewriteHandle::Simple(rewrite) => rewrite.clone(),
                BrokerRewriteHandle::SshTunnel(ssh_tunnel) => {
                    // The port for this can change over time, as the ssh tunnel is maintained through
                    // errors.
                    let addr = ssh_tunnel.local_addr();
                    BrokerRewrite {
                        host: addr.ip().to_string(),
                        port: Some(addr.port()),
                    }
                }
                BrokerRewriteHandle::FailedDefaultSshTunnel(_) => {
                    unreachable!()
                }
            };

            let new_addr = BrokerAddr {
                host: rewrite.host,
                port: match rewrite.port {
                    None => addr.port.clone(),
                    Some(port) => port.to_string(),
                },
            };
            info!(
                "rewriting broker {}:{} to {}:{}",
                addr.host, addr.port, new_addr.host, new_addr.port
            );
            new_addr
        };

        let rewrite = self.rewrites.lock().expect("poisoned").get(&addr).cloned();

        match rewrite {
            None | Some(BrokerRewriteHandle::FailedDefaultSshTunnel(_)) => {
                match &self.default_tunnel {
                    Some(default_tunnel) => {
                        // Multiple users could all run `connect` at the same time; only one ssh
                        // tunnel will ever be connected, and only one will be inserted into the
                        // map.
                        let ssh_tunnel = self.runtime.block_on(async {
                            self.ssh_tunnel_manager
                                .connect(
                                    default_tunnel.clone(),
                                    &addr.host,
                                    addr.port.parse().unwrap(),
                                )
                                .await
                        });
                        match ssh_tunnel {
                            Ok(ssh_tunnel) => {
                                let mut rewrites = self.rewrites.lock().expect("poisoned");
                                let rewrite = match rewrites.entry(addr.clone()) {
                                    btree_map::Entry::Occupied(mut o)
                                        if matches!(
                                            o.get(),
                                            BrokerRewriteHandle::FailedDefaultSshTunnel(_)
                                        ) =>
                                    {
                                        o.insert(BrokerRewriteHandle::SshTunnel(
                                            ssh_tunnel.clone(),
                                        ));
                                        o.into_mut()
                                    }
                                    btree_map::Entry::Occupied(o) => o.into_mut(),
                                    btree_map::Entry::Vacant(v) => {
                                        v.insert(BrokerRewriteHandle::SshTunnel(ssh_tunnel.clone()))
                                    }
                                };

                                return_rewrite(rewrite)
                            }
                            Err(e) => {
                                warn!(
                                    "failed to create ssh tunnel for {:?}: {}",
                                    addr,
                                    e.display_with_causes()
                                );

                                // Write an error if no one else has already written one.
                                let mut rewrites = self.rewrites.lock().expect("poisoned");
                                rewrites.entry(addr.clone()).or_insert_with(|| {
                                    BrokerRewriteHandle::FailedDefaultSshTunnel(
                                        e.to_string_with_causes(),
                                    )
                                });

                                // We have to give rdkafka an address, as this callback can't fail,
                                // we just give it a random one that will never resolve.
                                BrokerAddr {
                                    host: "failed-ssh-tunnel.dev.materialize.com".to_string(),
                                    port: 1337.to_string(),
                                }
                            }
                        }
                    }
                    None => addr,
                }
            }
            Some(rewrite) => return_rewrite(&rewrite),
        }
    }

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.inner.log(level, fac, log_message)
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.inner.error(error, reason)
    }

    fn stats(&self, statistics: Statistics) {
        self.inner.stats(statistics)
    }

    fn stats_raw(&self, statistics: &[u8]) {
        self.inner.stats_raw(statistics)
    }

    fn generate_oauth_token(
        &self,
        oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        self.inner.generate_oauth_token(oauthbearer_config)
    }
}

impl<C> ConsumerContext for TunnelingClientContext<C>
where
    C: ConsumerContext,
{
    fn rebalance(
        &self,
        native_client: &NativeClient,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        self.inner.rebalance(native_client, err, tpl)
    }

    fn pre_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        self.inner.pre_rebalance(rebalance)
    }

    fn post_rebalance<'a>(&self, rebalance: &Rebalance<'a>) {
        self.inner.post_rebalance(rebalance)
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        self.inner.commit_callback(result, offsets)
    }

    fn main_queue_min_poll_interval(&self) -> Timeout {
        self.inner.main_queue_min_poll_interval()
    }
}

impl<C> ProducerContext for TunnelingClientContext<C>
where
    C: ProducerContext,
{
    type DeliveryOpaque = C::DeliveryOpaque;

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        self.inner.delivery(delivery_result, delivery_opaque)
    }
}

/// Id of a partition in a topic.
pub type PartitionId = i32;

/// Retrieve number of partitions for a given `topic` using the given `client`
pub fn get_partitions<C: ClientContext>(
    client: &Client<C>,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<PartitionId>, anyhow::Error> {
    let meta = client.fetch_metadata(Some(topic), timeout)?;
    if meta.topics().len() != 1 {
        bail!(
            "topic {} has {} metadata entries; expected 1",
            topic,
            meta.topics().len()
        );
    }

    fn check_err(err: Option<RDKafkaRespErr>) -> anyhow::Result<()> {
        if let Some(err) = err {
            Err(RDKafkaErrorCode::from(err))?
        }
        Ok(())
    }

    let meta_topic = meta.topics().into_element();
    check_err(meta_topic.error())?;

    if meta_topic.name() != topic {
        bail!(
            "got results for wrong topic {} (expected {})",
            meta_topic.name(),
            topic
        );
    }

    let mut partition_ids = Vec::with_capacity(meta_topic.partitions().len());
    for partition_meta in meta_topic.partitions() {
        check_err(partition_meta.error())?;

        partition_ids.push(partition_meta.id());
    }

    if partition_ids.len() == 0 {
        bail!("topic {} does not exist", topic);
    }

    Ok(partition_ids)
}

/// A simpler version of [`create_new_client_config`] that defaults
/// the `log_level` to `INFO` and should only be used in tests.
pub fn create_new_client_config_simple() -> ClientConfig {
    create_new_client_config(tracing::Level::INFO)
}

/// Build a new [`rdkafka`] [`ClientConfig`] with its `log_level` set correctly
/// based on the passed through [`tracing::Level`]. This level should be
/// determined for `target: "librdkafka"`.
pub fn create_new_client_config(tracing_level: Level) -> ClientConfig {
    #[allow(clippy::disallowed_methods)]
    let mut config = ClientConfig::new();

    let level = if tracing_level >= Level::DEBUG {
        RDKafkaLogLevel::Debug
    } else if tracing_level >= Level::INFO {
        RDKafkaLogLevel::Info
    } else if tracing_level >= Level::WARN {
        RDKafkaLogLevel::Warning
    } else {
        RDKafkaLogLevel::Error
    };
    // WARNING WARNING WARNING
    //
    // For whatever reason, if you change this `target` to something else, this
    // log line might break. I (guswynn) did some extensive investigation with
    // the tracing folks, and we learned that this edge case only happens with
    // 1. a different target
    // 2. only this file (so far as I can tell)
    // 3. only in certain subscriber combinations
    // 4. only if the `tracing-log` feature is on.
    //
    // Our conclusion was that one of our dependencies is doing something
    // problematic with `log`.
    //
    // For now, this works, and prints a nice log line exactly when we want it.
    //
    // TODO(guswynn): when we can remove `tracing-log`, remove this warning
    tracing::debug!(target: "librdkafka", level = ?level, "Determined log level for librdkafka");
    config.set_log_level(level);

    // Patch the librdkafka debug log system into the Rust `log` ecosystem. This
    // is a very simple integration at the moment; enabling `debug`-level logs
    // for the `librdkafka` target enables the full firehouse of librdkafka
    // debug logs. We may want to investigate finer-grained control.
    if tracing_level >= Level::DEBUG {
        tracing::debug!(target: "librdkafka", "Enabling debug logs for rdkafka");
        config.set("debug", "all");
    }

    config
}
