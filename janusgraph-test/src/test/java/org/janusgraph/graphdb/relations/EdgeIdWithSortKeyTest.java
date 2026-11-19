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

package org.janusgraph.graphdb.relations;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EdgeIdWithSortKeyTest {

    private static final RelationIdentifier ID = new RelationIdentifier(1L, 2L, 3L, 4L);

    // Equality holds the same way round either way, so that membership does not depend on which side of the
    // comparison the wrapper is
    @Test
    public void equalsAnotherWrapperOfTheSameIdOnly() {
        final EdgeIdWithSortKey wrapped = new EdgeIdWithSortKey(ID, "0a0b");
        assertEquals(new EdgeIdWithSortKey(ID, "0c0d"), wrapped);
        assertEquals(ID.hashCode(), wrapped.hashCode());
        assertNotEquals(new EdgeIdWithSortKey(new RelationIdentifier(1L, 2L, 5L, 4L), "0a0b"), wrapped);

        assertNotEquals(ID, wrapped);
        assertNotEquals(wrapped, ID);
        final Set<Object> plainIds = new HashSet<>(Collections.singleton(ID));
        final Set<Object> wrappedIds = new HashSet<>(Collections.singleton(wrapped));
        assertFalse(plainIds.contains(wrapped));
        assertFalse(wrappedIds.contains(ID));
        assertTrue(plainIds.contains(EdgeIdWithSortKey.unwrap(wrapped)));
    }

    @Test
    public void unwrapReturnsThePlainId() {
        assertSame(ID, EdgeIdWithSortKey.unwrap(new EdgeIdWithSortKey(ID, "0a0b")));
        assertSame(ID, EdgeIdWithSortKey.unwrap(ID));
    }
}
