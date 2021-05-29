// Copyright 2020 JanusGraph Authors
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
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetricInstrumentedStoreManagerTest {

    @Test
    public void testGetHadoopManagerReturnsBaseHadoopManager() throws BackendException {
        Object result = new Object();
        KeyColumnValueStoreManager mock = mock(KeyColumnValueStoreManager.class);
        when(mock.getHadoopManager()).thenReturn(result);
        MetricInstrumentedStoreManager metricInstrumentedStoreManager = new MetricInstrumentedStoreManager(mock, "", true, "");

        Object hadoopManager = metricInstrumentedStoreManager.getHadoopManager();

        verify(mock).getHadoopManager();
        assertEquals(result, hadoopManager);
    }
}
