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

package org.janusgraph.graphdb;

import org.apache.commons.configuration2.MapConfiguration;
import org.janusgraph.diskstorage.configuration.ExecutorServiceBuilder;
import org.janusgraph.diskstorage.configuration.ExecutorServiceConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_CORE_POOL_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.PARALLEL_BACKEND_EXECUTOR_SERVICE_KEEP_ALIVE_TIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.PARALLEL_BACKEND_OPS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public abstract class GraphDatabaseConfigurationInstanceExecutorServiceTest {

    public abstract Map<String, Object> getStorageConfiguration();

    @Test
    public void shouldCreateCustomBackendFixedThreadPoolSize() {
        final Map<String, Object> map = getStorageConfiguration();
        map.put(UNIQUE_INSTANCE_ID_HOSTNAME.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_OPS.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CORE_POOL_SIZE.toStringWithoutRoot(), 15);
        final MapConfiguration config = ConfigurationUtil.loadMapConfiguration(map);
        assertDoesNotThrow(() -> {
            final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));
            graph.traversal().V().hasNext();
            graph.close();
        });
    }

    @Test
    public void shouldCreateCustomBackendCachedThreadPoolSize() {
        final Map<String, Object> map = getStorageConfiguration();
        map.put(UNIQUE_INSTANCE_ID_HOSTNAME.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_OPS.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS.toStringWithoutRoot(), ExecutorServiceBuilder.CACHED_THREAD_POOL_CLASS);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CORE_POOL_SIZE.toStringWithoutRoot(), 15);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_KEEP_ALIVE_TIME.toStringWithoutRoot(), 30000);
        final MapConfiguration config = ConfigurationUtil.loadMapConfiguration(map);
        assertDoesNotThrow(() -> {
            final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));
            graph.traversal().V().hasNext();
            graph.close();
        });
    }

    @Test
    public void shouldCreateCustomBackendExecutorServiceWithExecutorServiceConfigurationConstructor() {
        final Map<String, Object> map = getStorageConfiguration();
        map.put(UNIQUE_INSTANCE_ID_HOSTNAME.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_OPS.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CORE_POOL_SIZE.toStringWithoutRoot(), 15);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS.toStringWithoutRoot(), CustomExecutorServiceImplementation.class.getName());
        final MapConfiguration config = ConfigurationUtil.loadMapConfiguration(map);
        assertDoesNotThrow(() -> {
            final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));
            graph.traversal().V().hasNext();
            graph.close();
        });
    }

    @Test
    public void shouldCreateCustomBackendExecutorServiceWithoutExecutorServiceConfigurationConstructor() {
        final Map<String, Object> map = getStorageConfiguration();
        map.put(UNIQUE_INSTANCE_ID_HOSTNAME.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_OPS.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS.toStringWithoutRoot(), CustomParameterlessExecutorServiceImplementation.class.getName());
        final MapConfiguration config = ConfigurationUtil.loadMapConfiguration(map);
        assertDoesNotThrow(() -> {
            final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));
            graph.traversal().V().hasNext();
            graph.close();
        });
    }

    @Test
    public void shouldCreateCustomBackendExecutorServiceWithoutBothExecutorServiceConfigurationConstructors() {
        final Map<String, Object> map = getStorageConfiguration();
        map.put(UNIQUE_INSTANCE_ID_HOSTNAME.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_OPS.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CORE_POOL_SIZE.toStringWithoutRoot(), 15);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS.toStringWithoutRoot(), CustomExecutorServiceWithBothConstructorsImplementation.class.getName());
        final MapConfiguration config = ConfigurationUtil.loadMapConfiguration(map);
        assertDoesNotThrow(() -> {
            final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));
            graph.traversal().V().hasNext();
            graph.close();
        });
    }

    @Test
    public void shouldNotCreateBackendExecutorServiceIfClassNotExists() {
        String configurationClass = "not-existing-class";
        final Map<String, Object> map = getStorageConfiguration();
        map.put(UNIQUE_INSTANCE_ID_HOSTNAME.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_OPS.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS.toStringWithoutRoot(), configurationClass);

        checkExceptionIsThrownDuringInit(IllegalArgumentException.class, map,
            "No ExecutorService class found with class name: "+configurationClass);
    }

    @Test
    public void shouldNotCreateBackendExecutorServiceIfClassHasNoNecessaryConstructor() {
        String configurationClass = CustomExecutorServiceImplementationWithoutNecessaryConstructor.class.getName();
        final Map<String, Object> map = getStorageConfiguration();
        map.put(UNIQUE_INSTANCE_ID_HOSTNAME.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_OPS.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS.toStringWithoutRoot(), configurationClass);

        checkExceptionIsThrownDuringInit(IllegalArgumentException.class, map,
            configurationClass + " has neither public constructor which accepts "
                + ExecutorServiceConfiguration.class.getName() + " nor parameterless public constructor.");
    }

    @Test
    public void shouldNotCreateBackendExecutorServiceIfClassNotImplementsExecutorService() {
        String configurationClass = HashSet.class.getName();
        final Map<String, Object> map = getStorageConfiguration();
        map.put(UNIQUE_INSTANCE_ID_HOSTNAME.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_OPS.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS.toStringWithoutRoot(), configurationClass);

        checkExceptionIsThrownDuringInit(IllegalArgumentException.class, map,
            configurationClass + "isn't a subclass of "+ ExecutorService.class.getName());
    }

    @Test
    public void shouldNotCreateBackendExecutorServiceIfClassHasNoPublicConstructor() {
        String configurationClass = CustomExecutorServiceImplementationWithoutPublicConstructor.class.getName();
        final Map<String, Object> map = getStorageConfiguration();
        map.put(UNIQUE_INSTANCE_ID_HOSTNAME.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_OPS.toStringWithoutRoot(), true);
        map.put(PARALLEL_BACKEND_EXECUTOR_SERVICE_CLASS.toStringWithoutRoot(), configurationClass);

        checkExceptionIsThrownDuringInit(IllegalStateException.class, map,
            "Couldn't create a new instance of "+configurationClass +
                ". Please, check that the constructor which accepts "+ExecutorServiceConfiguration.class.getName()
                +" is public or there is a public parameterless constructor. If the necessary public constructor exists, " +
                "please, check that invocation of this constructor doesn't throw an exception.");
    }

    private <T extends Exception> void checkExceptionIsThrownDuringInit(Class<T> exceptionType, Map<String, Object> configMap, String message){
        final MapConfiguration config = ConfigurationUtil.loadMapConfiguration(configMap);
        T exception = assertThrows(exceptionType, () -> {
            final StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(new CommonsConfiguration(config)));
            graph.traversal().V().hasNext();
            graph.close();
        });
        assertEquals(message, exception.getMessage());
    }

    public static class CustomExecutorServiceImplementation extends ThreadPoolExecutor {
        public CustomExecutorServiceImplementation(ExecutorServiceConfiguration executorServiceConfiguration){
            super(executorServiceConfiguration.getCorePoolSize(), executorServiceConfiguration.getMaxPoolSize(),
                executorServiceConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
        }
    }

    public static class CustomParameterlessExecutorServiceImplementation extends ThreadPoolExecutor {
        public CustomParameterlessExecutorServiceImplementation(){
            super(1, 2, 3, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
        }
    }

    public static class CustomExecutorServiceWithBothConstructorsImplementation extends ThreadPoolExecutor {
        public CustomExecutorServiceWithBothConstructorsImplementation(){
            super(1, 2, 3, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
        }
        public CustomExecutorServiceWithBothConstructorsImplementation(ExecutorServiceConfiguration executorServiceConfiguration){
            super(executorServiceConfiguration.getCorePoolSize(), executorServiceConfiguration.getMaxPoolSize(),
                executorServiceConfiguration.getKeepAliveTime(), TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
        }
    }

    public static class CustomExecutorServiceImplementationWithoutNecessaryConstructor extends ThreadPoolExecutor {
        public CustomExecutorServiceImplementationWithoutNecessaryConstructor(int corePoolSize){
            super(corePoolSize, 2, 3, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
        }
    }

    public static class CustomExecutorServiceImplementationWithoutPublicConstructor extends ThreadPoolExecutor {
        private CustomExecutorServiceImplementationWithoutPublicConstructor(){
            super(1, 2, 3, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
        }
    }
}
