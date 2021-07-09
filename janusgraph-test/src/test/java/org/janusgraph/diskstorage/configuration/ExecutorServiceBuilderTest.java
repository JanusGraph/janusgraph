// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.diskstorage.configuration;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExecutorServiceBuilderTest {

    @Test
    public void shouldCreateCustomBackendCachedThreadPool() {

        ExecutorServiceConfiguration executorServiceConfiguration = new ExecutorServiceConfiguration();
        executorServiceConfiguration.setConfigurationClass(ExecutorServiceBuilder.CACHED_THREAD_POOL_CLASS);
        executorServiceConfiguration.setCorePoolSize(15);
        executorServiceConfiguration.setKeepAliveTime(30000L);
        assertDoesNotThrow(() -> ExecutorServiceBuilder.build(executorServiceConfiguration).shutdown());
    }

    @Test
    public void shouldCreateCustomBackendCachedThreadPoolWhenThreadFactoryIsProvided() {
        ExecutorServiceConfiguration executorServiceConfiguration = new ExecutorServiceConfiguration();
        executorServiceConfiguration.setConfigurationClass(ExecutorServiceBuilder.CACHED_THREAD_POOL_CLASS);
        executorServiceConfiguration.setCorePoolSize(15);
        executorServiceConfiguration.setKeepAliveTime(30000L);
        executorServiceConfiguration.setThreadFactory(Executors.defaultThreadFactory());
        assertDoesNotThrow(() -> ExecutorServiceBuilder.build(executorServiceConfiguration).shutdown());
    }

    @Test
    public void shouldNotCreateCustomBackendCachedThreadPoolWhenKeepAliveTimeIsNotProvided() {

        ExecutorServiceConfiguration executorServiceConfiguration = new ExecutorServiceConfiguration();
        executorServiceConfiguration.setConfigurationClass(ExecutorServiceBuilder.CACHED_THREAD_POOL_CLASS);
        executorServiceConfiguration.setCorePoolSize(15);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            ExecutorServiceBuilder.build(executorServiceConfiguration));

        assertEquals("To use "+ ExecutorServiceBuilder.CACHED_THREAD_POOL_CLASS+
            " executor service keepAliveTime must be provided.", exception.getMessage());
    }

    @Test
    public void shouldPassConfigurationToCustomClass() {

        String configurationClassName = CustomTestExecutorService.class.getName();
        int corePoolSize = 15;
        int maxPoolSize = 30;
        long keepAliveTime = 123456;
        ThreadFactory threadFactory = runnable -> null;

        ExecutorServiceConfiguration executorServiceConfiguration = new ExecutorServiceConfiguration(
            configurationClassName,
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            threadFactory
        );

        assertNull(CustomTestExecutorService.executorServiceConfiguration);

        assertDoesNotThrow(() -> ExecutorServiceBuilder.build(executorServiceConfiguration).shutdown());

        assertNotNull(CustomTestExecutorService.executorServiceConfiguration);

        assertEquals(corePoolSize, CustomTestExecutorService.executorServiceConfiguration.getCorePoolSize());
        assertEquals(maxPoolSize, CustomTestExecutorService.executorServiceConfiguration.getMaxPoolSize());
        assertEquals(keepAliveTime, CustomTestExecutorService.executorServiceConfiguration.getKeepAliveTime());
        assertEquals(threadFactory, CustomTestExecutorService.executorServiceConfiguration.getThreadFactory());
        assertEquals(configurationClassName, CustomTestExecutorService.executorServiceConfiguration.getConfigurationClass());
    }
}
