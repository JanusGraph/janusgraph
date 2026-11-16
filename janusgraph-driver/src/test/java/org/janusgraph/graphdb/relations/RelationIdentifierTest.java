// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.graphdb.relations;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RelationIdentifierTest {

    @Test
    public void testDisallowCustomVertexIdType() {
        long outVertexId = 12135151000L;
        long inVertexId = 21423513000L;
        long typeId = 123;
        long relationId = 500;
        RelationIdentifier rId = new RelationIdentifier(outVertexId, typeId, relationId, inVertexId);
        String serialized = rId.toString();
        RelationIdentifier deserialized = RelationIdentifier.parse(serialized);
        assertEquals(rId, deserialized);
        assertEquals(outVertexId, deserialized.getOutVertexId());
        assertEquals(inVertexId, deserialized.getInVertexId());
        assertEquals(typeId, deserialized.getTypeId());
        assertEquals(relationId, deserialized.getRelationId());
    }

    @Test
    public void testallowCustomVertexIdType() {
        String outVertexId = UUID.randomUUID().toString().replace('-', '_');
        long inVertexId = Long.MAX_VALUE;
        long typeId = 123;
        long relationId = 500;
        RelationIdentifier rId = new RelationIdentifier(outVertexId, typeId, relationId, inVertexId);
        String serialized = rId.toString();
        RelationIdentifier deserialized = RelationIdentifier.parse(serialized);
        assertEquals(rId, deserialized);
        assertEquals(outVertexId, deserialized.getOutVertexId());
        assertEquals(inVertexId, deserialized.getInVertexId());
        assertEquals(typeId, deserialized.getTypeId());
        assertEquals(relationId, deserialized.getRelationId());
    }

    private static final String D = RelationIdentifier.TOSTRING_DELIMITER;

    @Test
    public void testSortKeyRoundTripsThroughStringForm() {
        for (RelationIdentifier rId : new RelationIdentifier[]{
            new RelationIdentifier(4096L, 5L, 77L, 8200L),
            new RelationIdentifier("out_vertex", 5L, 77L, "in_vertex")}) {
            RelationIdentifier withSortKey = rId.withSortKey("00800000000000002a");
            assertEquals(rId.toString() + D + "K00800000000000002a", withSortKey.toString());

            RelationIdentifier parsed = RelationIdentifier.parse(withSortKey.toString());
            assertEquals("00800000000000002a", parsed.getSortKey());
            assertEquals(rId.getOutVertexId(), parsed.getOutVertexId());
            assertEquals(rId.getInVertexId(), parsed.getInVertexId());
            assertEquals(rId.getTypeId(), parsed.getTypeId());
            assertEquals(rId.getRelationId(), parsed.getRelationId());
            assertEquals(withSortKey.toString(), parsed.toString());
        }
    }

    @Test
    public void testIdentifierWithoutSortKeyIsUnchanged() {
        RelationIdentifier rId = new RelationIdentifier(4096L, 5L, 77L, 8200L);
        assertFalse(rId.hasSortKey());
        assertNull(RelationIdentifier.parse(rId.toString()).getSortKey());
        assertEquals(4, rId.toString().split(D).length);
        assertSame(rId, rId.withoutSortKey());
    }

    @Test
    public void testSortKeyIsNotPartOfIdentity() {
        RelationIdentifier rId = new RelationIdentifier(4096L, 5L, 77L, 8200L);
        RelationIdentifier withSortKey = rId.withSortKey("00");
        assertEquals(rId, withSortKey);
        assertEquals(withSortKey, rId);
        assertEquals(rId.hashCode(), withSortKey.hashCode());
        assertEquals(rId.toString(), withSortKey.withoutSortKey().toString());
    }

    @Test
    public void testInvalidSortKeysAreRejected() {
        RelationIdentifier edge = new RelationIdentifier(4096L, 5L, 77L, 8200L);
        for (String bad : new String[]{"", "0", "0g", "AB", "abc"}) {
            assertThrows(IllegalArgumentException.class, () -> edge.withSortKey(bad), bad);
            assertThrows(IllegalArgumentException.class, () -> RelationIdentifier.parse(edge + D + "K" + bad), bad);
        }
        assertThrows(IllegalArgumentException.class, () -> edge.withSortKey(null));
        // only edges have a sort key
        assertThrows(IllegalArgumentException.class, () -> new RelationIdentifier(4096L, 5L, 77L, null).withSortKey("00"));
        assertThrows(IllegalArgumentException.class, () -> RelationIdentifier.parse(edge + D + "x00"));
    }

    @Test
    public void testSortKeyHexEncoding() {
        byte[] bytes = {0, 1, 0x7f, (byte) 0x80, (byte) 0xff};
        assertEquals("00017f80ff", RelationIdentifier.encodeSortKey(bytes));
        assertArrayEquals(bytes, RelationIdentifier.decodeSortKey("00017f80ff"));
        assertTrue(RelationIdentifier.isValidSortKey("00017f80ff"));
        assertFalse(RelationIdentifier.isValidSortKey("00017F80FF"));
    }
}
