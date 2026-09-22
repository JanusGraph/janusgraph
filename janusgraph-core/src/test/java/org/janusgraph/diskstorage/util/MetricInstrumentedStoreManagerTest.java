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

package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.util.stats.MetricManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.janusgraph.diskstorage.util.MetricInstrumentedStore.M_CALLS;
import static org.janusgraph.diskstorage.util.MetricInstrumentedStore.M_MUTATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//A transaction which clears its metrics group name runs without metrics, and the wrapper must then hand the mutation
//to the backend exactly once and record nothing; with a group name it hands it over once and counts it. MetricManager
//is process wide, so each test meters under a manager name of its own
@ExtendWith(MockitoExtension.class)
public class MetricInstrumentedStoreManagerTest {

    @Mock
    private KeyColumnValueStoreManager backend;

    @Mock
    private StoreTransaction txh;

    private final String managerName = "storeManager." + UUID.randomUUID();

    @Test
    public void mutateManyShouldRunOnceAndRecordNothingWhenTheTransactionHasNoGroupName() throws BackendException {
        when(txh.getConfiguration()).thenReturn(configWithGroupName(null));
        final Map<String, Map<StaticBuffer, KCVMutation>> mutations = Collections.emptyMap();

        new MetricInstrumentedStoreManager(backend, managerName, false, "stores").mutateMany(mutations, txh);

        verify(backend, times(1)).mutateMany(mutations, txh);
        //The fallthrough counted the second run under the null group name this resolves to
        assertEquals(0, MetricManager.INSTANCE.getCounter(null, managerName, M_MUTATE, M_CALLS).getCount());
    }

    @Test
    public void mutateManyShouldRunOnceAndBeCountedWhenTheTransactionHasAGroupName() throws BackendException {
        final String group = "MetricInstrumentedStoreManagerTest." + UUID.randomUUID();
        when(txh.getConfiguration()).thenReturn(configWithGroupName(group));
        final Map<String, Map<StaticBuffer, KCVMutation>> mutations = Collections.emptyMap();

        new MetricInstrumentedStoreManager(backend, managerName, false, "stores").mutateMany(mutations, txh);

        verify(backend, times(1)).mutateMany(mutations, txh);
        assertEquals(1, MetricManager.INSTANCE.getCounter(group, managerName, M_MUTATE, M_CALLS).getCount());
    }

    private static BaseTransactionConfig configWithGroupName(String groupName) {
        return new StandardBaseTransactionConfig.Builder().timestampProvider(TimestampProviders.MILLI)
            .groupName(groupName).build();
    }
}
