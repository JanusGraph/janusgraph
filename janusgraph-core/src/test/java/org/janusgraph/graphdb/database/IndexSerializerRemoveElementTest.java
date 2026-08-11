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

package org.janusgraph.graphdb.database;

import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.graphdb.database.util.IndexRecordUtil;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

//removeElement is called by the transaction log recovery of StandardTransactionLogProcessor when the element of a
//restored transaction no longer exists, to remove its stale mixed index document. Rejecting a valid element id aborts
//that recovery, which is the mechanism that exists to repair a diverged index.
public class IndexSerializerRemoveElementTest {

    private static final String STORE_NAME = "vertexByName";

    private static IndexSerializer indexSerializer() {
        //removeElement reads neither the configuration nor the serializers
        return new IndexSerializer(null, null, null, null, false);
    }

    private static MixedIndexType index(ElementCategory elementCategory) {
        final MixedIndexType index = mock(MixedIndexType.class);
        when(index.getElement()).thenReturn(elementCategory);
        when(index.getStoreName()).thenReturn(STORE_NAME);
        return index;
    }

    private static List<IndexEntry> removeElement(Object elementId, ElementCategory elementCategory) {
        final Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new HashMap<>();
        indexSerializer().removeElement(elementId, index(elementCategory), documentsPerStore);
        final Map<String, List<IndexEntry>> documents = documentsPerStore.get(STORE_NAME);
        assertEquals(1, documents.size());
        final String documentId = documents.keySet().iterator().next();
        //The document id must identify the element that was removed
        assertEquals(elementId, IndexRecordUtil.string2ElementId(documentId));
        return documents.get(documentId);
    }

    @Test
    public void shouldRemoveVertexWithCustomStringId() {
        //A graph configured with graph.allow-custom-vid-types assigns String vertex ids. An empty entry list is how a
        //document removal is expressed
        assertTrue(removeElement("customVertexId", ElementCategory.VERTEX).isEmpty());
    }

    @Test
    public void shouldRemoveVertexWithLongId() {
        assertTrue(removeElement(42L, ElementCategory.VERTEX).isEmpty());
    }

    @Test
    public void shouldRemoveRelation() {
        assertTrue(removeElement(new RelationIdentifier(4L, 2L, 8L, 6L), ElementCategory.EDGE).isEmpty());
    }

    @Test
    public void shouldRejectIdOfWrongTypeForElementCategory() {
        final IndexSerializer indexSerializer = indexSerializer();
        final MixedIndexType vertexIndex = index(ElementCategory.VERTEX);
        final MixedIndexType edgeIndex = index(ElementCategory.EDGE);
        final Map<String, Map<String, List<IndexEntry>>> documents = new HashMap<>();

        //A relation id is not a vertex id, and a vertex id is not a relation id
        assertThrows(IllegalArgumentException.class, () -> indexSerializer.removeElement(
            new RelationIdentifier(4L, 2L, 8L, 6L), vertexIndex, documents));
        assertThrows(IllegalArgumentException.class,
            () -> indexSerializer.removeElement("customVertexId", edgeIndex, documents));
        assertThrows(IllegalArgumentException.class, () -> indexSerializer.removeElement(1.5, vertexIndex, documents));
    }
}
