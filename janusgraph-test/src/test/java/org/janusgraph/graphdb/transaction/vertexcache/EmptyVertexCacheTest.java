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

package org.janusgraph.graphdb.transaction.vertexcache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EmptyVertexCacheTest {
    @Test
    public void instanceIsSingleton() {
        assertTrue(EmptyVertexCache.getInstance() == EmptyVertexCache.getInstance());
    }

    @Test
    public void containsReturnsFalse() {
        assertFalse(EmptyVertexCache.getInstance().contains(1));
    }

    @Test
    public void getReturnsFalse() {
        assertEquals(null, EmptyVertexCache.getInstance().get(1, null));
    }

    @Test
    public void addDoesNotThrowException() {
        assertDoesNotThrow(() -> EmptyVertexCache.getInstance().add(null, 1));
    }

    @Test
    public void getAllNewReturnsEmpty() {
        assertTrue(EmptyVertexCache.getInstance().getAllNew().isEmpty());
    }
}
