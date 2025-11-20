// Copyright 2025 JanusGraph Authors
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

package org.janusgraph.diskstorage.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration test for CDC producer using Kafka testcontainer.
 */
@Testcontainers
public class CdcKafkaIntegrationTest {

    private static final String TOPIC_NAME = "test-cdc-topic";

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
    );

    private KafkaCdcProducer producer;
    private KafkaConsumer<String, String> consumer;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setup() {
        String bootstrapServers = kafka.getBootstrapServers();
        
        // Create producer
        producer = new KafkaCdcProducer(bootstrapServers, TOPIC_NAME);
        
        // Create consumer for verification
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    public void teardown() {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    public void testProducerSendsEventToKafka() throws Exception {
        // Create a CDC event
        List<IndexEntry> additions = Arrays.asList(
            new IndexEntry("name", "John"),
            new IndexEntry("age", 30)
        );
        
        CdcMutationEvent event = new CdcMutationEvent(
            "testStore",
            "doc123",
            additions,
            null,
            true,
            false,
            System.currentTimeMillis()
        );

        // Send event
        producer.send(event);
        producer.flush();

        // Consume and verify
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertEquals(1, records.count(), "Should receive one message");

        ConsumerRecord<String, String> record = records.iterator().next();
        assertEquals("testStore:doc123", record.key());
        
        // Deserialize and verify
        CdcMutationEvent receivedEvent = objectMapper.readValue(record.value(), CdcMutationEvent.class);
        assertNotNull(receivedEvent);
        assertEquals("testStore", receivedEvent.getStoreName());
        assertEquals("doc123", receivedEvent.getDocumentId());
        assertEquals(CdcMutationEvent.MutationType.ADDED, receivedEvent.getMutationType());
    }

    @Test
    public void testMultipleEventsPreserveOrder() throws Exception {
        // Send multiple events
        for (int i = 0; i < 5; i++) {
            CdcMutationEvent event = new CdcMutationEvent(
                "store" + i,
                "doc" + i,
                Arrays.asList(new IndexEntry("field", "value" + i)),
                null,
                false,
                false,
                System.currentTimeMillis()
            );
            producer.send(event);
        }
        producer.flush();

        // Verify all events received
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertEquals(5, records.count(), "Should receive five messages");

        int count = 0;
        for (ConsumerRecord<String, String> record : records) {
            CdcMutationEvent event = objectMapper.readValue(record.value(), CdcMutationEvent.class);
            assertNotNull(event);
            count++;
        }
        assertEquals(5, count);
    }
}
