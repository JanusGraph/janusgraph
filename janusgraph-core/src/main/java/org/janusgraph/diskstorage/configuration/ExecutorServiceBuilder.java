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

package org.janusgraph.diskstorage.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceBuilder {

    private static final Logger log = LoggerFactory.getLogger(ExecutorServiceBuilder.class);

    public static final int THREAD_POOL_SIZE_SCALE_FACTOR = 2;

    public static final String FIXED_THREAD_POOL_CLASS = "fixed";
    public static final String CACHED_THREAD_POOL_CLASS = "cached";

    private ExecutorServiceBuilder(){}

    public static ExecutorService build(ExecutorServiceConfiguration executorServiceConfiguration){

        switch (executorServiceConfiguration.getConfigurationClass()){

            case FIXED_THREAD_POOL_CLASS:
                return buildFixedExecutorService(toPoolSize(executorServiceConfiguration.getCorePoolSize()),
                    executorServiceConfiguration.getThreadFactory());

            case CACHED_THREAD_POOL_CLASS:
                return buildCachedExecutorService(
                    toPoolSize(executorServiceConfiguration.getCorePoolSize()),
                    executorServiceConfiguration.getKeepAliveTime(),
                    executorServiceConfiguration.getThreadFactory());

            default: return buildExecutorServiceFromClassName(executorServiceConfiguration);
        }
    }

    private static ExecutorService buildFixedExecutorService(int poolSize, ThreadFactory threadFactory){

        ExecutorService executorService;

        if(threadFactory == null){
            executorService = Executors.newFixedThreadPool(poolSize);
        } else {
            executorService = Executors.newFixedThreadPool(poolSize, threadFactory);
        }

        log.info("Initiated fixed thread pool of size {}", poolSize);

        return executorService;
    }

    private static ExecutorService buildCachedExecutorService(int corePoolSize, Long keepAliveTime,
                                                              ThreadFactory threadFactory){

        if(keepAliveTime == null){
            throw new IllegalArgumentException("To use "+CACHED_THREAD_POOL_CLASS+
                " executor service keepAliveTime must be provided.");
        }

        ThreadPoolExecutor threadPoolExecutor;

        if(threadFactory == null){
            threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, Integer.MAX_VALUE, keepAliveTime, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
        } else {
            threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, Integer.MAX_VALUE, keepAliveTime, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), threadFactory);
        }

        threadPoolExecutor.prestartAllCoreThreads();

        log.info("Initiated cached thread pool with {} pre-started core size threads and keep alive time of {} ms",
            corePoolSize, keepAliveTime);

        return threadPoolExecutor;
    }

    private static ExecutorService buildExecutorServiceFromClassName(ExecutorServiceConfiguration executorServiceConfiguration){

        String configurationClass = executorServiceConfiguration.getConfigurationClass();

        Class<?> executorServiceClass;
        try {
            executorServiceClass = Class.forName(configurationClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("No ExecutorService class found with class name: "+configurationClass);
        }

        if(!ExecutorService.class.isAssignableFrom(executorServiceClass)){
            throw new IllegalArgumentException(configurationClass + "isn't a subclass of "+ExecutorService.class.getName());
        }

        ExecutorService executorService;
        Constructor<?> parameterlessConstructor = null;
        Constructor<?> executorServiceConfigurationConstructor = null;

        for (Constructor<?> constructor : executorServiceClass.getDeclaredConstructors()) {
            if (constructor.getParameterCount() == 1){
                if(ExecutorServiceConfiguration.class.isAssignableFrom(constructor.getParameterTypes()[0])){
                    executorServiceConfigurationConstructor = constructor;
                    break;
                }
            } else if(constructor.getParameterCount() == 0){
                parameterlessConstructor = constructor;
            }
        }

        try {

            if(executorServiceConfigurationConstructor != null){
                executorService = (ExecutorService) executorServiceConfigurationConstructor.newInstance(executorServiceConfiguration);
            } else {
                if(parameterlessConstructor == null){
                    throw new IllegalArgumentException(configurationClass + " has neither public constructor which accepts "
                        +ExecutorServiceConfiguration.class.getName() + " nor parameterless public constructor.");
                }
                executorService = (ExecutorService) parameterlessConstructor.newInstance();
            }

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Couldn't create a new instance of "+configurationClass +
                ". Please, check that the constructor which accepts "+ExecutorServiceConfiguration.class.getName()
                +" is public or there is a public parameterless constructor. If the necessary public constructor exists, " +
                "please, check that invocation of this constructor doesn't throw an exception.", e);
        }

        log.info("Initiated custom executor service {}", configurationClass);

        return executorService;
    }

    private static int toPoolSize(Integer poolSize){
        return poolSize != null ? poolSize :
            Runtime.getRuntime().availableProcessors() * THREAD_POOL_SIZE_SCALE_FACTOR;
    }
}
