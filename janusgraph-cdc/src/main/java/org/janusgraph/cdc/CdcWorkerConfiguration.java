// Copyright 2026 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.cdc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Immutable configuration for the {@link CdcIndexUpdateWorker}: Kafka connection/consumer-group settings, worker
 * concurrency, the poll timeout, and the retry/backoff policy used when applying index updates fails transiently.
 */
public final class CdcWorkerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(CdcWorkerConfiguration.class);

    public static final String BOOTSTRAP_SERVERS = "cdc.bootstrap-servers";
    public static final String TOPICS = "cdc.topics";
    public static final String GROUP_ID = "cdc.group-id";
    public static final String MAX_POLL_RECORDS = "cdc.max-poll-records";
    public static final String POLL_TIMEOUT_MS = "cdc.poll-timeout-ms";
    public static final String WORKER_THREADS = "cdc.worker-threads";
    public static final String RETRY_LIMIT = "cdc.retry.limit";
    public static final String RETRY_INITIAL_WAIT_MS = "cdc.retry.initial-wait-ms";
    public static final String RETRY_MAX_WAIT_MS = "cdc.retry.max-wait-ms";
    /** Path to the JanusGraph configuration file the worker opens (read access to the graph plus index-backend write
     *  access). Consumed by {@link CdcIndexUpdateWorkerMain}; declared here so all {@code cdc.*} keys live together. */
    public static final String GRAPH_CONFIG = "cdc.graph-config";
    /** Prefix for arbitrary Kafka consumer settings (e.g. {@code cdc.consumer.security.protocol=SASL_SSL}): the rest
     *  of the key is passed to the {@link org.apache.kafka.clients.consumer.KafkaConsumer} verbatim. The settings the
     *  worker's at-least-once contract depends on (manual offset commits, byte-array deserializers) and the settings
     *  with dedicated {@code cdc.*} keys (bootstrap servers, group id, max poll records) cannot be overridden this
     *  way; everything else -- including {@code auto.offset.reset}, which merely defaults to {@code earliest} -- can. */
    public static final String CONSUMER_PREFIX = "cdc.consumer.";

    private static final Set<String> KNOWN_KEYS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        BOOTSTRAP_SERVERS, TOPICS, GROUP_ID, MAX_POLL_RECORDS, POLL_TIMEOUT_MS, WORKER_THREADS,
        RETRY_LIMIT, RETRY_INITIAL_WAIT_MS, RETRY_MAX_WAIT_MS, GRAPH_CONFIG)));

    private final String bootstrapServers;
    private final List<String> topics;
    private final String groupId;
    private final int maxPollRecords;
    private final Duration pollTimeout;
    private final int workerThreads;
    private final int retryLimit;
    private final Duration retryInitialWait;
    private final Duration retryMaxWait;
    private final Map<String, String> consumerOverrides;

    private CdcWorkerConfiguration(Builder b) {
        this.bootstrapServers = Objects.requireNonNull(b.bootstrapServers, BOOTSTRAP_SERVERS + " is required");
        if (b.topics == null || b.topics.isEmpty()) {
            throw new IllegalArgumentException(TOPICS + " is required");
        }
        this.topics = Collections.unmodifiableList(new ArrayList<>(b.topics));
        this.groupId = Objects.requireNonNull(b.groupId, GROUP_ID + " must not be null");
        this.maxPollRecords = requireAtLeast(b.maxPollRecords, 1, MAX_POLL_RECORDS);
        this.pollTimeout = requirePositive(b.pollTimeout, POLL_TIMEOUT_MS);
        this.workerThreads = requireAtLeast(b.workerThreads, 1, WORKER_THREADS);
        this.retryLimit = requireAtLeast(b.retryLimit, 0, RETRY_LIMIT);
        this.retryInitialWait = requireNonNegative(b.retryInitialWait, RETRY_INITIAL_WAIT_MS);
        this.retryMaxWait = requireNonNegative(b.retryMaxWait, RETRY_MAX_WAIT_MS);
        this.consumerOverrides = Collections.unmodifiableMap(new LinkedHashMap<>(b.consumerOverrides));
    }

    private static int requireAtLeast(int value, int min, String name) {
        if (value < min) {
            throw new IllegalArgumentException(name + " must be >= " + min + " but was " + value);
        }
        return value;
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isNegative() || value.isZero()) {
            // Fail fast: a negative poll timeout would make every consumer.poll() throw (an endless error-log loop
            // instead of a startup failure), and a ZERO poll timeout would busy-spin the poll loop on an idle topic
            // (poll(ZERO) returns immediately), pinning a core per worker and hammering the broker with fetches.
            throw new IllegalArgumentException(name + " must be positive but was " + value.toMillis() + " ms");
        }
        return value;
    }

    private static Duration requireNonNegative(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isNegative()) {
            // Fail fast: negative retry waits would disable the backoff entirely.
            throw new IllegalArgumentException(name + " must not be negative but was " + value.toMillis() + " ms");
        }
        return value;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public List<String> getTopics() {
        return topics;
    }

    public String getGroupId() {
        return groupId;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public Duration getRetryInitialWait() {
        return retryInitialWait;
    }

    public Duration getRetryMaxWait() {
        return retryMaxWait;
    }

    /** Extra Kafka consumer settings passed through from {@code cdc.consumer.*} keys (prefix stripped). */
    public Map<String, String> getConsumerOverrides() {
        return consumerOverrides;
    }

    /** Builds the Kafka {@link org.apache.kafka.clients.consumer.KafkaConsumer} properties. Auto-commit is disabled so
     *  the worker can commit offsets only after a batch has been durably applied to the index (at-least-once). */
    public Properties toConsumerProperties() {
        Properties p = new Properties();
        // Pass-through settings first (security, timeouts, client.id, ...): the managed settings below always win,
        // because either the worker's at-least-once contract depends on them (manual offset commits, raw byte
        // payloads) or they have a dedicated first-class cdc.* key (bootstrap servers, group id, max poll records).
        consumerOverrides.forEach(p::put);
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // A DEFAULT, not managed: "earliest" so a brand-new consumer group starts from the retained CDC backlog
        // instead of Kafka's "latest" default (which would silently drop every change captured before first start).
        // Overridable via cdc.consumer.auto.offset.reset (e.g. "latest" when a fresh group's history is covered by a
        // REINDEX) -- it does not affect the at-least-once contract, only where a group with no offsets begins.
        p.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return p;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static CdcWorkerConfiguration fromProperties(Properties props) {
        Builder b = builder()
            .bootstrapServers(props.getProperty(BOOTSTRAP_SERVERS))
            .topics(splitTopics(props.getProperty(TOPICS)));
        ifSet(props, GROUP_ID, b::groupId);
        ifSet(props, MAX_POLL_RECORDS, v -> b.maxPollRecords(Integer.parseInt(v)));
        ifSet(props, POLL_TIMEOUT_MS, v -> b.pollTimeout(Duration.ofMillis(Long.parseLong(v))));
        ifSet(props, WORKER_THREADS, v -> b.workerThreads(Integer.parseInt(v)));
        ifSet(props, RETRY_LIMIT, v -> b.retryLimit(Integer.parseInt(v)));
        ifSet(props, RETRY_INITIAL_WAIT_MS, v -> b.retryInitialWait(Duration.ofMillis(Long.parseLong(v))));
        ifSet(props, RETRY_MAX_WAIT_MS, v -> b.retryMaxWait(Duration.ofMillis(Long.parseLong(v))));
        final Map<String, String> consumerOverrides = new LinkedHashMap<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(CONSUMER_PREFIX)) {
                consumerOverrides.put(key.substring(CONSUMER_PREFIX.length()), props.getProperty(key).trim());
            } else if (key.startsWith("cdc.") && !KNOWN_KEYS.contains(key)) {
                // A typo'd option would otherwise run silently with the default value the operator believes they tuned.
                log.warn("Ignoring unrecognized configuration key '{}' (known keys: {}; Kafka consumer settings can be "
                    + "passed as {}<consumer-property>)", key, KNOWN_KEYS, CONSUMER_PREFIX);
            }
        }
        b.consumerProperties(consumerOverrides);
        return b.build();
    }

    private static void ifSet(Properties props, String key, Consumer<String> setter) {
        final String value = props.getProperty(key);
        if (value != null) {
            setter.accept(value.trim());
        }
    }

    private static List<String> splitTopics(String csv) {
        if (csv == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(csv.split(","))
            .map(String::trim)
            .filter(t -> !t.isEmpty())
            .collect(Collectors.toList());
    }

    public static final class Builder {
        private String bootstrapServers;
        private List<String> topics;
        private String groupId = "janusgraph-cdc";
        private int maxPollRecords = 500;
        private Duration pollTimeout = Duration.ofMillis(1000);
        private int workerThreads = 1;
        private int retryLimit = 5;
        private Duration retryInitialWait = Duration.ofMillis(100);
        private Duration retryMaxWait = Duration.ofMillis(30000);
        private Map<String, String> consumerOverrides = Collections.emptyMap();

        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder topics(List<String> topics) {
            this.topics = topics;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder maxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
            return this;
        }

        public Builder pollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
            return this;
        }

        public Builder workerThreads(int workerThreads) {
            this.workerThreads = workerThreads;
            return this;
        }

        public Builder retryLimit(int retryLimit) {
            this.retryLimit = retryLimit;
            return this;
        }

        public Builder retryInitialWait(Duration retryInitialWait) {
            this.retryInitialWait = retryInitialWait;
            return this;
        }

        public Builder retryMaxWait(Duration retryMaxWait) {
            this.retryMaxWait = retryMaxWait;
            return this;
        }

        /** Extra Kafka consumer settings (keys WITHOUT the {@code cdc.consumer.} prefix), e.g. security settings. */
        public Builder consumerProperties(Map<String, String> consumerOverrides) {
            this.consumerOverrides = Objects.requireNonNull(consumerOverrides, "consumerOverrides must not be null");
            return this;
        }

        public CdcWorkerConfiguration build() {
            return new CdcWorkerConfiguration(this);
        }
    }
}
