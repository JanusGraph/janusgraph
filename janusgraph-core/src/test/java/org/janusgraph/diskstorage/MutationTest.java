// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.diskstorage;

import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MutationTest {

    private static KCVMutation empty() {
        return new KCVMutation(KeyColumnValueStore.NO_ADDITIONS, KeyColumnValueStore.NO_DELETIONS);
    }

    @Test
    public void defaultsToNoWholeRowDeletion() {
        assertFalse(empty().hasWholeRowDeletion());
    }

    @Test
    public void wholeRowDeletionMakesMutationNonEmpty() {
        KCVMutation m = empty();
        assertTrue(m.isEmpty());
        m.setWholeRowDeletion(true);
        assertTrue(m.hasWholeRowDeletion());
        assertFalse(m.isEmpty());
        assertEquals(1, m.getTotalMutations());
    }

    @Test
    public void mergeOrsWholeRowDeletion() {
        KCVMutation a = empty();
        KCVMutation b = empty();
        b.setWholeRowDeletion(true);
        a.merge(b);
        assertTrue(a.hasWholeRowDeletion());
    }

    @Test
    public void mergeKeepsWholeRowDeletionWhenAlreadySet() {
        KCVMutation a = empty();
        a.setWholeRowDeletion(true);
        KCVMutation b = empty(); // b has no whole-row deletion
        a.merge(b);
        assertTrue(a.hasWholeRowDeletion());
    }

    @Test
    public void mergeWholeRowDeletionBothSet() {
        KCVMutation a = empty();
        a.setWholeRowDeletion(true);
        KCVMutation b = empty();
        b.setWholeRowDeletion(true);
        a.merge(b);
        assertTrue(a.hasWholeRowDeletion());
    }
}
