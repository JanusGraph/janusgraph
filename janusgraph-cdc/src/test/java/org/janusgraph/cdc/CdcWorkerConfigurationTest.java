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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CdcWorkerConfigurationTest {

    @Test
    public void builderAppliesDefaults() {
        CdcWorkerConfiguration cfg = CdcWorkerConfiguration.builder()
            .bootstrapServers("kafka:9092")
            .topics(Arrays.asList("cassandra.janusgraph.edgestore"))
            .build();

        assertEquals("kafka:9092", cfg.getBootstrapServers());
        assertEquals(Arrays.asList("cassandra.janusgraph.edgestore"), cfg.getTopics());
        assertEquals("janusgraph-cdc", cfg.getGroupId());
        assertEquals(500, cfg.getMaxPollRecords());
        assertEquals(1, cfg.getWorkerThreads());
        assertEquals(Duration.ofMillis(1000), cfg.getPollTimeout());
        assertEquals(5, cfg.getRetryLimit());
    }

    @Test
    public void fromPropertiesParsesAllKeys() {
        Properties p = new Properties();
        p.setProperty("cdc.bootstrap-servers", "a:9092,b:9092");
        p.setProperty("cdc.topics", "t1,t2");
        p.setProperty("cdc.group-id", "grp");
        p.setProperty("cdc.max-poll-records", "250");
        p.setProperty("cdc.poll-timeout-ms", "2000");
        p.setProperty("cdc.worker-threads", "4");
        p.setProperty("cdc.retry.limit", "9");
        p.setProperty("cdc.retry.initial-wait-ms", "50");
        p.setProperty("cdc.retry.max-wait-ms", "60000");

        CdcWorkerConfiguration cfg = CdcWorkerConfiguration.fromProperties(p);

        assertEquals("a:9092,b:9092", cfg.getBootstrapServers());
        assertEquals(Arrays.asList("t1", "t2"), cfg.getTopics());
        assertEquals("grp", cfg.getGroupId());
        assertEquals(250, cfg.getMaxPollRecords());
        assertEquals(4, cfg.getWorkerThreads());
        assertEquals(Duration.ofMillis(2000), cfg.getPollTimeout());
        assertEquals(9, cfg.getRetryLimit());
        assertEquals(Duration.ofMillis(50), cfg.getRetryInitialWait());
        assertEquals(Duration.ofMillis(60000), cfg.getRetryMaxWait());
    }

    @Test
    public void consumerPropertiesDisableAutoCommitAndUseByteArrayDeserializers() {
        CdcWorkerConfiguration cfg = CdcWorkerConfiguration.builder()
            .bootstrapServers("kafka:9092")
            .topics(Arrays.asList("t"))
            .groupId("grp")
            .maxPollRecords(123)
            .build();

        Properties consumer = cfg.toConsumerProperties();
        assertEquals("kafka:9092", consumer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("grp", consumer.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(123, consumer.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals(false, consumer.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals(ByteArrayDeserializer.class.getName(), consumer.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(ByteArrayDeserializer.class.getName(), consumer.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertFalse(consumer.containsKey("cdc.bootstrap-servers"), "internal keys must not leak into consumer config");
    }

    @Test
    public void rejectsInvalidConfiguration() {
        // Fail fast on misconfiguration rather than deferring to Kafka consumer construction or misbehaving silently.
        assertThrows(NullPointerException.class, () -> baseBuilder().groupId(null).build());
        assertThrows(NullPointerException.class, () -> baseBuilder().pollTimeout(null).build());
        assertThrows(NullPointerException.class, () -> baseBuilder().retryInitialWait(null).build());
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().workerThreads(0).build());
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().maxPollRecords(0).build());
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().retryLimit(-1).build());
        // A negative poll timeout would make every consumer.poll() throw (an endless error-log loop instead of a
        // startup failure), a ZERO poll timeout would busy-spin the poll loop on an idle topic, and negative retry
        // waits would silently disable the backoff. Zero retry waits are legal ("retry immediately").
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().pollTimeout(Duration.ofMillis(-1)).build());
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().pollTimeout(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().retryInitialWait(Duration.ofMillis(-1)).build());
        assertThrows(IllegalArgumentException.class, () -> baseBuilder().retryMaxWait(Duration.ofMillis(-1)).build());
        baseBuilder().retryInitialWait(Duration.ZERO).retryMaxWait(Duration.ZERO).build();
    }

    @Test
    public void passesThroughConsumerPropertiesAndIgnoresUnknownKeys() {
        Properties p = new Properties();
        p.setProperty("cdc.bootstrap-servers", "kafka:9092");
        p.setProperty("cdc.topics", "t");
        p.setProperty("cdc.consumer.security.protocol", "SASL_SSL");     // pass-through (prefix stripped)
        p.setProperty("cdc.consumer.enable.auto.commit", "true");        // must NOT override the managed setting
        p.setProperty("cdc.retry.limt", "9");                            // typo: warned and ignored, default applies

        CdcWorkerConfiguration cfg = CdcWorkerConfiguration.fromProperties(p);

        Properties consumer = cfg.toConsumerProperties();
        assertEquals("SASL_SSL", consumer.get("security.protocol"));
        assertEquals(false, consumer.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG),
            "the at-least-once offset management must win over pass-through settings");
        assertEquals(5, cfg.getRetryLimit(), "a typo'd cdc.* key must not be applied");
    }

    @Test
    public void autoOffsetResetIsADefaultNotAManagedSetting() {
        assertEquals("earliest", baseBuilder().build().toConsumerProperties()
                .get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
            "a new consumer group must default to the retained CDC backlog, not Kafka's 'latest'");

        Properties p = new Properties();
        p.setProperty("cdc.bootstrap-servers", "kafka:9092");
        p.setProperty("cdc.topics", "t");
        p.setProperty("cdc.consumer.auto.offset.reset", "latest"); // e.g. fresh group whose history a REINDEX covers
        assertEquals("latest", CdcWorkerConfiguration.fromProperties(p).toConsumerProperties()
                .get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
            "auto.offset.reset is only a default and must honor a cdc.consumer.* override");
    }

    private CdcWorkerConfiguration.Builder baseBuilder() {
        return CdcWorkerConfiguration.builder()
            .bootstrapServers("kafka:9092")
            .topics(Arrays.asList("t"));
    }
}
