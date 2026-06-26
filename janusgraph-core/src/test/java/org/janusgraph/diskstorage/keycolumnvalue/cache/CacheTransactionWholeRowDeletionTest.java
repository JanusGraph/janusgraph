// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.diskstorage.keycolumnvalue.cache;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CacheTransactionWholeRowDeletionTest {

    @SuppressWarnings("unchecked")
    @Test
    public void wholeRowDeletionFlagReachesBackend() throws BackendException {
        KeyColumnValueStoreManager manager = mock(KeyColumnValueStoreManager.class);
        KeyColumnValueStore rawStore = mock(KeyColumnValueStore.class);
        when(rawStore.getName()).thenReturn("edgestore");
        NoKCVSCache cache = new NoKCVSCache(rawStore);
        StoreTransaction wrappedTx = mock(StoreTransaction.class);
        CacheTransaction ctx = new CacheTransaction(wrappedTx, manager, 100, 100, Duration.ofSeconds(10), false);

        // Capture a deep copy of the mutations map before persist() clears it.
        // Mockito ArgumentCaptor captures by reference, which is cleared after mutateMany returns,
        // so we use doAnswer to snapshot the contents at call time.
        AtomicReference<Map<String, Map<StaticBuffer, KCVMutation>>> captured = new AtomicReference<>();
        doAnswer(invocation -> {
            Map<String, Map<StaticBuffer, KCVMutation>> arg =
                (Map<String, Map<StaticBuffer, KCVMutation>>) invocation.getArgument(0);
            // Shallow copy of submutations map taken before persist() clears it; the KCVMutation values are not mutated afterward.
            Map<String, Map<StaticBuffer, KCVMutation>> snapshot = new HashMap<>();
            for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> e : arg.entrySet()) {
                snapshot.put(e.getKey(), new HashMap<>(e.getValue()));
            }
            captured.set(snapshot);
            return null;
        }).when(manager).mutateMany(any(), any());

        StaticBuffer key = BufferUtil.getIntBuffer(1);
        cache.mutateEntries(key, KeyColumnValueStore.NO_ADDITIONS, KCVSCache.NO_DELETIONS, true, ctx);
        ctx.commit();

        Map<String, Map<StaticBuffer, KCVMutation>> mutations = captured.get();
        assertNotNull(mutations, "mutateMany was not called");
        Map<StaticBuffer, KCVMutation> storeMutations = mutations.get("edgestore");
        assertNotNull(storeMutations, "no mutations for 'edgestore'");
        KCVMutation m = storeMutations.get(key);
        assertNotNull(m, "no mutation for key");
        assertTrue(m.hasWholeRowDeletion(), "wholeRowDeletion flag must be true");
        assertTrue(m.getDeletions().isEmpty(), "deletions must be empty for a whole-row deletion");
    }
}
