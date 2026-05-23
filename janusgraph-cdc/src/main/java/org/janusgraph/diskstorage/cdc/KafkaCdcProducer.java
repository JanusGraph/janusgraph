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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka-based implementation of CDC producer.
 * Serializes CDC events as JSON and sends them to a Kafka topic.
 */
public class KafkaCdcProducer implements CdcProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaCdcProducer.class);
    private static final long SEND_TIMEOUT_MS = 30000; // 30 seconds

    private final KafkaProducer<String, String> producer;
    private final String topicName;
    private final ObjectMapper objectMapper;

    public KafkaCdcProducer(String bootstrapServers, String topicName) {
        this(createDefaultProperties(bootstrapServers), topicName);
    }

    public KafkaCdcProducer(Properties kafkaProperties, String topicName) {
        this.topicName = topicName;
        this.objectMapper = new ObjectMapper();
        this.producer = new KafkaProducer<>(kafkaProperties);
        log.info("Initialized KafkaCdcProducer for topic: {}", topicName);
    }

    private static Properties createDefaultProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        return props;
    }

    @Override
    public void send(CdcMutationEvent event) throws BackendException {
        try {
            String key = event.getStoreName() + ":" + event.getDocumentId();
            String value = objectMapper.writeValueAsString(event);
            
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send CDC event to Kafka topic {}: {}", topicName, event, exception);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Successfully sent CDC event to topic {} partition {} offset {}",
                                topicName, metadata.partition(), metadata.offset());
                    }
                }
            }).get(SEND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TemporaryBackendException("Interrupted while sending CDC event", e);
        } catch (ExecutionException e) {
            throw new PermanentBackendException("Failed to send CDC event to Kafka", e.getCause());
        } catch (TimeoutException e) {
            throw new TemporaryBackendException("Timeout while sending CDC event to Kafka", e);
        } catch (Exception e) {
            throw new PermanentBackendException("Error serializing or sending CDC event", e);
        }
    }

    @Override
    public void flush() throws BackendException {
        try {
            producer.flush();
        } catch (Exception e) {
            throw new TemporaryBackendException("Failed to flush Kafka producer", e);
        }
    }

    @Override
    public void close() {
        try {
            producer.close();
            log.info("Closed KafkaCdcProducer for topic: {}", topicName);
        } catch (Exception e) {
            log.error("Error closing Kafka producer", e);
        }
    }
}
