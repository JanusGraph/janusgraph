// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb.idmanagement;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class UniqueInstanceIdRetrieverConcurrencyTest {

    @Mock
    private Configuration config;

    @Test
    public void shouldComputeDifferentUidSuffixInConcurrentEnv() throws ExecutionException, InterruptedException {
        final int concurrency = 5;
        ExecutorService es = Executors.newFixedThreadPool(concurrency);
        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            futures.add(es.submit(() -> {
                return UniqueInstanceIdRetriever.getInstance().getOrGenerateUniqueInstanceId(config);
            }));
        }
        Set<String> ids = new HashSet<>();
        for (Future<String> future : futures) {
            ids.add(future.get());
        }
        assertEquals(concurrency, ids.size(), "Generated ids are: " + ids);
        es.shutdown();
    }
}

