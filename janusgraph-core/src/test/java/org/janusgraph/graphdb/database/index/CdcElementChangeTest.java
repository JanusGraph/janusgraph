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

package org.janusgraph.graphdb.database.index;

import org.janusgraph.graphdb.internal.ElementCategory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * The equals/hashCode contract matters operationally: the CDC worker de-duplicates a poll batch via a
 * {@code LinkedHashSet&lt;CdcElementChange&gt;}, so value equality is what collapses duplicate events.
 */
public class CdcElementChangeTest {

    @Test
    public void equalityIsByCategoryAndElementId() {
        assertEquals(new CdcElementChange(ElementCategory.VERTEX, 42L),
            new CdcElementChange(ElementCategory.VERTEX, 42L));
        assertEquals(new CdcElementChange(ElementCategory.VERTEX, 42L).hashCode(),
            new CdcElementChange(ElementCategory.VERTEX, 42L).hashCode());
        assertNotEquals(new CdcElementChange(ElementCategory.VERTEX, 42L),
            new CdcElementChange(ElementCategory.VERTEX, 43L));
        assertNotEquals(new CdcElementChange(ElementCategory.VERTEX, 42L),
            new CdcElementChange(ElementCategory.PROPERTY, 42L));
    }
}
