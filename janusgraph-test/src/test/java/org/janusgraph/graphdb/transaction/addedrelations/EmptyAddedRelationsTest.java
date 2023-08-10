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

package org.janusgraph.graphdb.transaction.addedrelations;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EmptyAddedRelationsTest {
    @Test
    public void instanceIsSingleton() {
        assertTrue(EmptyAddedRelations.getInstance() == EmptyAddedRelations.getInstance());
    }

    @Test
    public void cannotAdd() {
        assertFalse(EmptyAddedRelations.getInstance().add(null));
    }

    @Test
    public void cannotRemove() {
        assertFalse(EmptyAddedRelations.getInstance().remove(null));
    }

    @Test
    public void isEmpty() {
        assertTrue(EmptyAddedRelations.getInstance().isEmpty());
    }

    @Test
    public void getAllUnsafeReturnsEmpty() {
        assertTrue(EmptyAddedRelations.getInstance().getAllUnsafe().isEmpty());
    }

    @Test
    public void getViewReturnsEmpty() {
        assertFalse(EmptyAddedRelations.getInstance().getView(null).iterator().hasNext());
    }
}
