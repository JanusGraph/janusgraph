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

import org.janusgraph.graphdb.database.index.CdcElementChange;

import java.util.Collection;

/**
 * Decodes a single raw Change-Data-Capture record (as delivered to the CDC worker over Kafka) into zero or more
 * {@link CdcElementChange}s identifying which graph elements changed. The capture source determines the wire format;
 * {@link DebeziumCassandraJsonDecoder} handles Debezium's Cassandra change events for the edgestore table.
 *
 * <p>Implementations must be thread-safe (a worker may share one decoder across consumer threads) and side-effect free.</p>
 */
public interface CdcEventDecoder {

    /**
     * @param messageKey   the Kafka record key bytes (may be {@code null})
     * @param messageValue the Kafka record value bytes (may be {@code null} for tombstones)
     * @return the element changes implied by this record; empty if the record is irrelevant (e.g. a non-edgestore
     *         table, a schema event, or a Kafka tombstone)
     */
    Collection<CdcElementChange> decode(byte[] messageKey, byte[] messageValue);
}
