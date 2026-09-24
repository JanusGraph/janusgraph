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
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.util.stats.MetricManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.janusgraph.diskstorage.util.MetricInstrumentedIndexProvider.M_CALLS;
import static org.janusgraph.diskstorage.util.MetricInstrumentedIndexProvider.M_MUTATE;
import static org.janusgraph.diskstorage.util.MetricInstrumentedIndexProvider.M_RESTORE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//A transaction which clears its metrics group name, TransactionBuilder.groupName(null), runs without metrics. The
//wrapper must then hand the operation to the provider exactly once and record nothing, as the callable overload
//which serves the query methods already does. Before the fix the unmetered run fell through into the metered path,
//which ran the operation again and counted it under a null group name. MetricManager is process wide, so each test
//meters under a prefix of its own and reads back only counters it could have written
@ExtendWith(MockitoExtension.class)
public class MetricInstrumentedIndexProviderTest {

    @Mock
    private IndexProvider indexProvider;

    @Mock
    private KeyInformation.IndexRetriever information;

    @Mock
    private BaseTransactionConfigurable tx;

    private final String prefix = "indexProvider." + UUID.randomUUID();

    @Test
    public void mutateShouldRunOnceAndRecordNothingWhenTheTransactionHasNoGroupName() throws BackendException {
        when(tx.getConfiguration()).thenReturn(configWithGroupName(null));
        final Map<String, Map<String, IndexMutation>> mutations = Collections.emptyMap();

        new MetricInstrumentedIndexProvider(indexProvider, prefix).mutate(mutations, information, tx);

        verify(indexProvider, times(1)).mutate(mutations, information, tx);
        //The fallthrough counted the second run under the null group name this resolves to
        assertEquals(0, MetricManager.INSTANCE.getCounter(null, prefix, M_MUTATE, M_CALLS).getCount());
    }

    @Test
    public void restoreShouldRunOnceAndRecordNothingWhenTheTransactionHasNoGroupName() throws BackendException {
        when(tx.getConfiguration()).thenReturn(configWithGroupName(null));
        final Map<String, Map<String, List<IndexEntry>>> documents = Collections.emptyMap();

        new MetricInstrumentedIndexProvider(indexProvider, prefix).restore(documents, information, tx);

        verify(indexProvider, times(1)).restore(documents, information, tx);
        assertEquals(0, MetricManager.INSTANCE.getCounter(null, prefix, M_RESTORE, M_CALLS).getCount());
    }

    @Test
    public void mutateShouldRunOnceAndBeCountedWhenTheTransactionHasAGroupName() throws BackendException {
        final String group = "MetricInstrumentedIndexProviderTest." + UUID.randomUUID();
        when(tx.getConfiguration()).thenReturn(configWithGroupName(group));
        final Map<String, Map<String, IndexMutation>> mutations = Collections.emptyMap();

        new MetricInstrumentedIndexProvider(indexProvider, prefix).mutate(mutations, information, tx);

        verify(indexProvider, times(1)).mutate(mutations, information, tx);
        assertEquals(1, MetricManager.INSTANCE.getCounter(group, prefix, M_MUTATE, M_CALLS).getCount());
    }

    private static BaseTransactionConfig configWithGroupName(String groupName) {
        return new StandardBaseTransactionConfig.Builder().timestampProvider(TimestampProviders.MILLI)
            .groupName(groupName).build();
    }
}
