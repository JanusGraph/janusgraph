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

import org.janusgraph.diskstorage.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BackendTest {
    @Test
    public void testThreadsNamed() {
        ExecutorService executorService = Backend.buildExecutorService(Configuration.EMPTY);
        try {
            executorService.execute(() -> {
                String threadName = Thread.currentThread().getName();
                assertNotNull(threadName);
                assertFalse(threadName.isEmpty());
                assertTrue(threadName.toLowerCase().startsWith("backend"));
            });
        } finally {
            assertDoesNotThrow(executorService::shutdown);
        }
    }
}
