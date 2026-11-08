use std::time::Duration;

use rdkafka::config::ClientConfig;

use crate::{ClientSettings, Error, Topic};

/// Membership protocols have distinct broker and client prerequisites.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Membership {
    /// Traditional eager assignment, compatible with older Kafka groups.
    Classic,
    /// Classic group protocol with cooperative-sticky partition movement.
    Cooperative,
    /// Kafka 4 consumer group protocol (KIP-848), not just cooperative rebalance.
    Consumer,
}

/// One credential-free final-resolution diagnostic per removed option.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConfigurationWarning {
    /// KIP-848 delegates this classic-only option to the broker instead.
    RemovedClassicSetting(&'static str),
}

/// Final native configuration and the warnings that its caller should report.
/// Native configuration is deliberately not formatted: it may hold credentials.
pub struct ResolvedConsumerConfig {
    pub(crate) native: ClientConfig,
    pub(crate) warnings: Vec<ConfigurationWarning>,
}

impl ResolvedConsumerConfig {
    /// Final client settings, without opening a native connection.
    pub fn native(&self) -> &ClientConfig {
        &self.native
    }

    /// One warning for each explicitly configured classic-only option removed.
    pub fn warnings(&self) -> &[ConfigurationWarning] {
        &self.warnings
    }
}

/// Offset custody strategies never enable automatic storing of fetched records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitStrategy {
    /// Store only a safe custody watermark; the native auto-committer flushes it.
    StoreThenAutoFlush(Duration),
    /// Confirm each newly completed safe prefix synchronously with the broker.
    PerMessage,
    /// Commit after a bounded number of new completions since a successful flush.
    /// Retained completed records do not count again. A full admission
    /// window must also flush so an oversized batch cannot deadlock progress.
    BatchCount(u16),
    /// Flush safe completed prefixes periodically, without waiting for new input.
    Interval(Duration),
}

/// Explicit configured group and topic subscriptions; wire headers cannot select
/// a group, membership protocol, tenant client or native bootstrap destination.
pub struct ConsumerSettings {
    pub(crate) group: String,
    pub(crate) topics: Vec<Topic>,
    pub(crate) membership: Membership,
    pub(crate) instance: Option<String>,
    pub(crate) strategy: CommitStrategy,
    pub(crate) from_beginning: bool,
}

impl ConsumerSettings {
    /// Declares one group with explicit subscription and custody behavior.
    pub fn try_new(
        group: impl Into<String>,
        topics: Vec<Topic>,
        membership: Membership,
        instance: Option<String>,
        strategy: CommitStrategy,
    ) -> Result<Self, Error> {
        let group = group.into();
        if !identifier(&group)
            || topics.is_empty()
            || topics.len() > 256
            || instance.as_deref().is_some_and(|value| !identifier(value))
        {
            return Err(Error::Configuration);
        }
        let mut names: Vec<_> = topics.iter().map(Topic::as_str).collect();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::Configuration);
        }
        match strategy {
            CommitStrategy::BatchCount(0) => return Err(Error::Configuration),
            CommitStrategy::Interval(value) | CommitStrategy::StoreThenAutoFlush(value)
                if !(Duration::from_millis(10)..=Duration::from_secs(300)).contains(&value) =>
            {
                return Err(Error::Configuration);
            }
            _ => {}
        }
        Ok(Self {
            group,
            topics,
            membership,
            instance,
            strategy,
            from_beginning: false,
        })
    }

    /// Selects the missing-offset fallback only. Existing group offsets are never
    /// reset by this setting; independent replay uses a separately owned group.
    pub fn from_beginning(mut self) -> Self {
        self.from_beginning = true;
        self
    }

    /// Builds the exact provider settings without connecting or starting tasks.
    /// Use resolve to also retain KIP-848 normalization warnings.
    pub fn native_config(&self, settings: &ClientSettings) -> Result<ClientConfig, Error> {
        Ok(self.resolve(settings)?.native)
    }

    /// Resolves explicit membership, preserving static identity and unrelated
    /// native options. KIP-848 removes incompatible classic settings with one
    /// typed warning per removal; contradictory explicit protocols still fail.
    pub fn resolve(&self, settings: &ClientSettings) -> Result<ResolvedConsumerConfig, Error> {
        let mut native = settings.native.clone();
        let mut warnings = Vec::new();
        let expected = if self.membership == Membership::Consumer {
            "consumer"
        } else {
            "classic"
        };
        if native
            .get("group.protocol")
            .is_some_and(|value| value != expected)
            || native
                .get("group.id")
                .is_some_and(|value| value != self.group)
            || native.get("group.instance.id").is_some_and(|value| {
                !identifier(value)
                    || self
                        .instance
                        .as_deref()
                        .is_some_and(|instance| value != instance)
            })
        {
            return Err(Error::Configuration);
        }
        if self.membership == Membership::Consumer {
            for unsupported in [
                "partition.assignment.strategy",
                "session.timeout.ms",
                "heartbeat.interval.ms",
                "group.protocol.type",
            ] {
                if native.get(unsupported).is_some() {
                    native.remove(unsupported);
                    warnings.push(ConfigurationWarning::RemovedClassicSetting(unsupported));
                }
            }
        } else if native.get("group.remote.assignor").is_some() {
            return Err(Error::Configuration);
        }
        let assignor = match self.membership {
            Membership::Classic => Some("range"),
            Membership::Cooperative => Some("cooperative-sticky"),
            Membership::Consumer => None,
        };
        if let Some(assignor) = assignor {
            if native
                .get("partition.assignment.strategy")
                .is_some_and(|value| value != assignor)
            {
                return Err(Error::Configuration);
            }
            native.set("partition.assignment.strategy", assignor);
        }
        native
            .set("group.protocol", expected)
            .set("group.id", &self.group)
            .set("enable.auto.offset.store", "false")
            .set(
                "auto.offset.reset",
                if self.from_beginning {
                    "earliest"
                } else {
                    "latest"
                },
            );
        if let Some(instance) = &self.instance {
            native.set("group.instance.id", instance);
        }
        match self.strategy {
            CommitStrategy::StoreThenAutoFlush(interval) => {
                native
                    .set("enable.auto.commit", "true")
                    .set("auto.commit.interval.ms", interval.as_millis().to_string());
            }
            _ => {
                native.set("enable.auto.commit", "false");
            }
        }
        // Wolverine defaults to read_uncommitted, whereas bundled librdkafka
        // 2.12.1 defaults to read_committed. Preserve the explicit opt-in.
        if native.get("isolation.level").is_none() {
            native.set("isolation.level", "read_uncommitted");
        }
        native
            .set(
                "fetch.max.bytes",
                (settings.limits.max_record_bytes + 4096)
                    .max(1024)
                    .to_string(),
            )
            .set(
                "fetch.message.max.bytes",
                settings.limits.max_record_bytes.max(1024).to_string(),
            )
            .set(
                "queued.max.messages.kbytes",
                ((settings.limits.max_record_bytes * settings.limits.max_pending) / 1024)
                    .max(1)
                    .to_string(),
            );
        Ok(ResolvedConsumerConfig { native, warnings })
    }

    /// Configured subscription; no application-message metadata is inspected.
    pub fn topics(&self) -> &[Topic] {
        &self.topics
    }
}

fn identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 249 && !value.chars().any(char::is_control)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{CommitStrategy, ConfigurationWarning, ConsumerSettings, Membership};
    use crate::{ClientSettings, Limits, Topic};
    use rdkafka::client::ClientContext;
    use rdkafka::config::ClientConfig;
    use rdkafka::config::RDKafkaLogLevel;
    use rdkafka::consumer::{BaseConsumer, ConsumerContext};
    use rdkafka::error::KafkaError;
    use std::time::Duration;

    struct ConstructionContext;
    impl ClientContext for ConstructionContext {
        fn log(&self, _: RDKafkaLogLevel, _: &str, _: &str) {}
        fn error(&self, _: KafkaError, _: &str) {}
    }
    impl ConsumerContext for ConstructionContext {}

    #[test]
    fn constructed_options_pass_native_cross_property_validation_without_network() {
        for membership in [
            Membership::Classic,
            Membership::Cooperative,
            Membership::Consumer,
        ] {
            let settings = client(|_| {});
            let mut native = subscription(membership).native_config(&settings).unwrap();
            // Configuration construction must not contact a developer's broker.
            // Retain all cross-property options but remove every bootstrap route.
            native.set("bootstrap.servers", "");
            let consumer: BaseConsumer<ConstructionContext> =
                native.create_with_context(ConstructionContext).unwrap();
            drop(consumer);
        }
    }

    fn client(configure: impl FnOnce(&mut ClientConfig)) -> ClientSettings {
        let mut native = ClientConfig::new();
        native
            .set("bootstrap.servers", "127.0.0.1:9092")
            .set("security.protocol", "plaintext");
        configure(&mut native);
        ClientSettings::try_new(native, Limits::default()).unwrap()
    }
    fn subscription(membership: Membership) -> ConsumerSettings {
        ConsumerSettings::try_new(
            "test-group",
            vec![Topic::try_new("test-topic").unwrap()],
            membership,
            None,
            CommitStrategy::PerMessage,
        )
        .unwrap()
    }
    #[test]
    fn membership_does_not_confuse_next_generation_and_cooperative_protocols() {
        let cooperative = subscription(Membership::Cooperative)
            .native_config(&client(|_| {}))
            .unwrap();
        assert_eq!(cooperative.get("group.protocol"), Some("classic"));
        assert_eq!(
            cooperative.get("partition.assignment.strategy"),
            Some("cooperative-sticky")
        );
        let consumer = subscription(Membership::Consumer)
            .native_config(&client(|_| {}))
            .unwrap();
        assert_eq!(consumer.get("group.protocol"), Some("consumer"));
        assert_eq!(consumer.get("partition.assignment.strategy"), None);
        assert_eq!(consumer.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(consumer.get("enable.auto.commit"), Some("false"));
        assert_eq!(consumer.get("isolation.level"), Some("read_uncommitted"));
        assert_eq!(consumer.get("allow.auto.create.topics"), Some("false"));
    }
    #[test]
    fn isolation_defaults_to_uncommitted_and_preserves_committed_opt_in() {
        let subscription = subscription(Membership::Classic);
        assert_eq!(
            subscription
                .native_config(&client(|_| {}))
                .unwrap()
                .get("isolation.level"),
            Some("read_uncommitted")
        );
        assert_eq!(
            subscription
                .native_config(&client(|native| {
                    native.set("isolation.level", "read_committed");
                }))
                .unwrap()
                .get("isolation.level"),
            Some("read_committed")
        );
    }
    #[test]
    fn every_strategy_disables_fetched_offset_storage_and_only_store_enables_auto_commit() {
        for strategy in [
            CommitStrategy::StoreThenAutoFlush(Duration::from_secs(1)),
            CommitStrategy::PerMessage,
            CommitStrategy::BatchCount(10),
            CommitStrategy::Interval(Duration::from_secs(1)),
        ] {
            let subscription = ConsumerSettings::try_new(
                "safe-group",
                vec![Topic::try_new("safe-topic").unwrap()],
                Membership::Classic,
                None,
                strategy,
            )
            .unwrap();
            let config = subscription
                .native_config(&client(|native| {
                    native.set("enable.auto.offset.store", "true");
                    native.set("enable.auto.commit", "true");
                }))
                .unwrap();
            assert_eq!(config.get("enable.auto.offset.store"), Some("false"));
            assert_eq!(
                config.get("enable.auto.commit"),
                Some(
                    if matches!(strategy, CommitStrategy::StoreThenAutoFlush(_)) {
                        "true"
                    } else {
                        "false"
                    }
                )
            );
        }
    }
    #[test]
    fn consumer_protocol_normalizes_classic_options_once_and_preserves_identity() {
        let removed = [
            "partition.assignment.strategy",
            "session.timeout.ms",
            "heartbeat.interval.ms",
            "group.protocol.type",
        ];
        let settings = client(|native| {
            for key in removed {
                native.set(key, "42");
            }
            native.set("group.instance.id", "stable-member");
            native.set("fetch.min.bytes", "17");
        });
        let resolved = subscription(Membership::Consumer)
            .resolve(&settings)
            .unwrap();
        assert_eq!(
            resolved.warnings(),
            removed.map(ConfigurationWarning::RemovedClassicSetting)
        );
        for key in removed {
            assert_eq!(resolved.native().get(key), None);
        }
        assert_eq!(
            resolved.native().get("group.instance.id"),
            Some("stable-member")
        );
        assert_eq!(resolved.native().get("fetch.min.bytes"), Some("17"));
        assert_eq!(settings.native.get("group.protocol.type"), Some("42"));
        let classic = subscription(Membership::Classic)
            .resolve(&client(|native| {
                native.set("group.protocol", "classic");
                native.set("session.timeout.ms", "45000");
                native.set("heartbeat.interval.ms", "3000");
                native.set("group.protocol.type", "consumer");
            }))
            .unwrap();
        assert!(classic.warnings().is_empty());
        assert_eq!(classic.native().get("session.timeout.ms"), Some("45000"));
        assert_eq!(classic.native().get("heartbeat.interval.ms"), Some("3000"));
        assert_eq!(
            classic.native().get("group.protocol.type"),
            Some("consumer")
        );
    }
    #[test]
    fn contradictory_explicit_protocols_and_assignors_fail() {
        assert!(
            subscription(Membership::Classic)
                .native_config(&client(|native| {
                    native.set("group.remote.assignor", "range");
                }))
                .is_err()
        );
        assert!(
            subscription(Membership::Cooperative)
                .native_config(&client(|native| {
                    native.set("partition.assignment.strategy", "range");
                }))
                .is_err()
        );
        assert!(
            subscription(Membership::Consumer)
                .native_config(&client(|native| {
                    native.set("group.protocol", "classic");
                }))
                .is_err()
        );
    }
}
