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

import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for CDC classes.
 */
public class CdcMutationEventTest {

    @Test
    public void testCdcMutationEventCreation() {
        List<IndexEntry> additions = Arrays.asList(
            new IndexEntry("field1", "value1"),
            new IndexEntry("field2", "value2")
        );
        List<IndexEntry> deletions = Arrays.asList(
            new IndexEntry("field3", "value3")
        );

        CdcMutationEvent event = new CdcMutationEvent(
            "testStore",
            "doc123",
            additions,
            deletions,
            true,
            false,
            System.currentTimeMillis()
        );

        assertEquals("testStore", event.getStoreName());
        assertEquals("doc123", event.getDocumentId());
        assertEquals(2, event.getAdditions().size());
        assertEquals(1, event.getDeletions().size());
        assertTrue(event.isNew());
        assertFalse(event.isDeleted());
        assertEquals(CdcMutationEvent.MutationType.ADDED, event.getMutationType());
    }

    @Test
    public void testMutationTypeDetection() {
        // Test ADDED type
        CdcMutationEvent addedEvent = new CdcMutationEvent(
            "store", "doc1", null, null, true, false, System.currentTimeMillis()
        );
        assertEquals(CdcMutationEvent.MutationType.ADDED, addedEvent.getMutationType());

        // Test UPDATED type
        CdcMutationEvent updatedEvent = new CdcMutationEvent(
            "store", "doc2", null, null, false, false, System.currentTimeMillis()
        );
        assertEquals(CdcMutationEvent.MutationType.UPDATED, updatedEvent.getMutationType());

        // Test DELETED type
        CdcMutationEvent deletedEvent = new CdcMutationEvent(
            "store", "doc3", null, null, false, true, System.currentTimeMillis()
        );
        assertEquals(CdcMutationEvent.MutationType.DELETED, deletedEvent.getMutationType());
    }

    @Test
    public void testEqualsAndHashCode() {
        long timestamp = System.currentTimeMillis();
        List<IndexEntry> additions = Arrays.asList(new IndexEntry("field1", "value1"));
        
        CdcMutationEvent event1 = new CdcMutationEvent(
            "store", "doc1", additions, null, true, false, timestamp
        );
        CdcMutationEvent event2 = new CdcMutationEvent(
            "store", "doc1", additions, null, true, false, timestamp
        );
        
        assertEquals(event1, event2);
        assertEquals(event1.hashCode(), event2.hashCode());
    }
}
