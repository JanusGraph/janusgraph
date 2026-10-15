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

package org.janusgraph.diskstorage.es;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ElasticSearchPagingModeTest {

    @Test
    public void shouldAcceptEveryModeByItsConfigName() {
        for (final ElasticSearchPagingMode mode : ElasticSearchPagingMode.values()) {
            assertEquals(mode, ElasticSearchPagingMode.fromConfigName(mode.getConfigName()));
            assertEquals(mode.getConfigName(), ElasticSearchIndex.PAGING_MODE.verify(mode.getConfigName()));
        }
    }

    //A mode is named exactly, as JanusGraph's other mode options are
    @Test
    public void shouldRejectAnyOtherName() {
        for (final String name : Arrays.asList("SCROLL", "point-in-time", "adaptive", "")) {
            assertNull(ElasticSearchPagingMode.fromConfigName(name), name);
            assertThrows(IllegalArgumentException.class, () -> ElasticSearchIndex.PAGING_MODE.verify(name), name);
        }
        assertNull(ElasticSearchPagingMode.fromConfigName(null));
    }

    @Test
    public void shouldReadAdaptivelyByDefault() {
        assertEquals(ElasticSearchPagingMode.ADAPTIVE_POINT_IN_TIME,
            ElasticSearchPagingMode.fromConfigName(ElasticSearchIndex.PAGING_MODE.getDefaultValue()));
    }
}
