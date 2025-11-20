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
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * CDC worker that consumes CDC events from Kafka and applies mutations to mixed indexes.
 * This ensures eventual consistency between the storage backend and mixed indexes.
 */
public class CdcWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CdcWorker.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final KafkaConsumer<String, String> consumer;
    private final IndexProvider indexProvider;
    private final KeyInformation.IndexRetriever indexRetriever;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running;
    private final Thread workerThread;

    public CdcWorker(String bootstrapServers, String topicName, String groupId,
                    IndexProvider indexProvider, KeyInformation.IndexRetriever indexRetriever) {
        this(createDefaultProperties(bootstrapServers, groupId), topicName, indexProvider, indexRetriever);
    }

    public CdcWorker(Properties consumerProperties, String topicName,
                    IndexProvider indexProvider, KeyInformation.IndexRetriever indexRetriever) {
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.consumer.subscribe(Collections.singletonList(topicName));
        this.indexProvider = indexProvider;
        this.indexRetriever = indexRetriever;
        this.objectMapper = new ObjectMapper();
        this.running = new AtomicBoolean(false);
        this.workerThread = new Thread(this::run, "CDC-Worker-" + topicName);
        log.info("Initialized CdcWorker for topic: {}", topicName);
    }

    private static Properties createDefaultProperties(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        return props;
    }

    /**
     * Start the CDC worker in a separate thread.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            workerThread.start();
            log.info("Started CDC worker thread");
        }
    }

    /**
     * Main processing loop for CDC events.
     */
    private void run() {
        log.info("CDC worker thread started");
        while (running.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (!records.isEmpty()) {
                    processBatch(records);
                    consumer.commitSync();
                }
            } catch (Exception e) {
                log.error("Error processing CDC events", e);
                // Continue processing on error
            }
        }
        log.info("CDC worker thread stopped");
    }

    /**
     * Process a batch of CDC events.
     */
    private void processBatch(ConsumerRecords<String, String> records) {
        Map<String, Map<String, IndexMutation>> mutations = new HashMap<>();
        
        for (ConsumerRecord<String, String> record : records) {
            try {
                CdcMutationEvent event = objectMapper.readValue(record.value(), CdcMutationEvent.class);
                processEvent(event, mutations);
            } catch (Exception e) {
                log.error("Failed to deserialize or process CDC event from offset {}: {}",
                        record.offset(), record.value(), e);
            }
        }
        
        // Apply all mutations to the index
        if (!mutations.isEmpty()) {
            applyMutations(mutations);
        }
    }

    /**
     * Process a single CDC event and accumulate mutations.
     */
    private void processEvent(CdcMutationEvent event, Map<String, Map<String, IndexMutation>> mutations) {
        String storeName = event.getStoreName();
        String documentId = event.getDocumentId();
        
        // Get or create the store mutations map
        Map<String, IndexMutation> storeMutations = mutations.computeIfAbsent(storeName, k -> new HashMap<>());
        
        // Get or create the document mutation
        IndexMutation mutation = storeMutations.get(documentId);
        if (mutation == null) {
            mutation = new IndexMutation(
                    indexRetriever.get(storeName),
                    event.getAdditions(),
                    event.getDeletions(),
                    event.isNew(),
                    event.isDeleted()
            );
            storeMutations.put(documentId, mutation);
        } else {
            // Merge with existing mutation
            IndexMutation newMutation = new IndexMutation(
                    indexRetriever.get(storeName),
                    event.getAdditions(),
                    event.getDeletions(),
                    event.isNew(),
                    event.isDeleted()
            );
            mutation.merge(newMutation);
        }
        
        if (log.isDebugEnabled()) {
            log.debug("Processed CDC event: {}", event);
        }
    }

    /**
     * Apply accumulated mutations to the index provider.
     */
    private void applyMutations(Map<String, Map<String, IndexMutation>> mutations) {
        try {
            BaseTransaction tx = indexProvider.beginTransaction(null);
            try {
                indexProvider.mutate(mutations, indexRetriever, tx);
                tx.commit();
                log.info("Successfully applied {} store mutations from CDC", mutations.size());
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        } catch (BackendException e) {
            log.error("Failed to apply CDC mutations to index", e);
            throw new RuntimeException("Failed to apply CDC mutations", e);
        }
    }

    /**
     * Stop the CDC worker gracefully.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            try {
                workerThread.join(5000);
                log.info("Stopped CDC worker thread");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while stopping CDC worker");
            }
        }
    }

    @Override
    public void close() {
        stop();
        try {
            consumer.close();
            log.info("Closed CDC worker");
        } catch (Exception e) {
            log.error("Error closing Kafka consumer", e);
        }
    }
}
