// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.diskstorage.cql.strategy;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.CQLStoreManager;
import org.janusgraph.diskstorage.util.backpressure.builder.QueryBackPressureBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class GroupedExecutionStrategyBuilder {

    private static final Logger log = LoggerFactory.getLogger(QueryBackPressureBuilder.class);

    public static final String TOKEN_RANGE_AWARE = "tokenRangeAware";
    public static final String REPLICAS_AWARE = "replicasAware";

    private GroupedExecutionStrategyBuilder(){}

    public static GroupedExecutionStrategy build(final Configuration configuration, final CQLStoreManager storeManager, final String className){

        switch (className){
            case TOKEN_RANGE_AWARE: return new TokenRangeAwareGroupedExecutionStrategy(configuration, storeManager);
            case REPLICAS_AWARE: return new ReplicasAwareGroupedExecutionStrategy(configuration, storeManager);
            default: return buildFromClassName(className, configuration, storeManager);
        }
    }

    private static GroupedExecutionStrategy buildFromClassName(final String className, final Configuration configuration, final CQLStoreManager storeManager){


        Class<?> implementationClass;
        try {
            implementationClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("No class found with class name: "+className);
        }

        if(!GroupedExecutionStrategy.class.isAssignableFrom(implementationClass)){
            throw new IllegalArgumentException(className + "isn't a subclass of "+GroupedExecutionStrategy.class.getName());
        }

        final GroupedExecutionStrategy result;
        Constructor<?> constructorWithConfigurationAndSessionParams = null;

        for (Constructor<?> constructor : implementationClass.getDeclaredConstructors()) {
            if (constructor.getParameterCount() == 2 && Configuration.class.isAssignableFrom(constructor.getParameterTypes()[0]) &&
                CQLStoreManager.class.isAssignableFrom(constructor.getParameterTypes()[1])){
                constructorWithConfigurationAndSessionParams = constructor;
                break;
            }
        }

        try {

            if(constructorWithConfigurationAndSessionParams == null){
                throw new IllegalArgumentException(className + " has no a public constructor which accepts "
                    +Configuration.class.getName() + " and "+CQLStoreManager.class.getName()+" parameters.");
            }

            result = (GroupedExecutionStrategy) constructorWithConfigurationAndSessionParams.newInstance(configuration, storeManager);

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Couldn't create a new instance of "+className +
                ". Please, check that the constructor which accepts "+Configuration.class.getName()+" and "+CQLStoreManager.class.getName()
                +" is public. If the necessary public constructor exists, please, check that invocation of this constructor doesn't throw an exception.", e);
        }

        log.info("Initiated custom GroupedExecutionStrategy {}", className);

        return result;
    }

}
