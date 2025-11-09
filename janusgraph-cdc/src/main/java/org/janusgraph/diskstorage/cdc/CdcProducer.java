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

import org.janusgraph.diskstorage.BackendException;

/**
 * Interface for producing CDC events to Kafka topics.
 * Implementations should handle serialization and delivery of CDC events.
 */
public interface CdcProducer extends AutoCloseable {

    /**
     * Send a CDC mutation event to the configured Kafka topic.
     *
     * @param event The CDC mutation event to send
     * @throws BackendException if the event cannot be sent
     */
    void send(CdcMutationEvent event) throws BackendException;

    /**
     * Flush any pending events to ensure they are sent.
     *
     * @throws BackendException if flush fails
     */
    void flush() throws BackendException;

    /**
     * Close the producer and release resources.
     */
    @Override
    void close();
}
