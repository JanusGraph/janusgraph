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

package org.janusgraph.diskstorage.util.backpressure.builder;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.util.backpressure.PassAllQueryBackPressure;
import org.janusgraph.diskstorage.util.backpressure.QueryBackPressure;
import org.janusgraph.diskstorage.util.backpressure.SemaphoreProtectedReleaseQueryBackPressure;
import org.janusgraph.diskstorage.util.backpressure.SemaphoreQueryBackPressure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class QueryBackPressureBuilder {

    private static final Logger log = LoggerFactory.getLogger(QueryBackPressureBuilder.class);

    public static final String SEMAPHORE_QUERY_BACK_PRESSURE_CLASS = "semaphore";
    public static final String SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS = "semaphoreReleaseProtected";
    public static final String PASS_ALL_QUERY_BACK_PRESSURE_CLASS = "passAll";

    private QueryBackPressureBuilder(){}

    public static QueryBackPressure build(final Configuration configuration, final String className, int backPressureLimit){

        switch (className){

            case SEMAPHORE_QUERY_BACK_PRESSURE_CLASS:
                return new SemaphoreQueryBackPressure(backPressureLimit);

            case SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS:
                return new SemaphoreProtectedReleaseQueryBackPressure(backPressureLimit);

            case PASS_ALL_QUERY_BACK_PRESSURE_CLASS:
                return new PassAllQueryBackPressure();

            default: return buildQueryBackPressureFromClassName(className, configuration, backPressureLimit);
        }
    }

    private static QueryBackPressure buildQueryBackPressureFromClassName(final String className, final Configuration configuration, Integer backPressureLimit){


        Class<?> queryBackPressureClass;
        try {
            queryBackPressureClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("No QueryBackPressure class found with class name: "+className);
        }

        if(!QueryBackPressure.class.isAssignableFrom(queryBackPressureClass)){
            throw new IllegalArgumentException(className + "isn't a subclass of "+QueryBackPressure.class.getName());
        }

        final QueryBackPressure queryBackPressure;
        Constructor<?> parameterlessConstructor = null;
        Constructor<?> constructorWithConfigurationParam = null;
        Constructor<?> constructorWithConfigurationAndBackpressureParams = null;

        for (Constructor<?> constructor : queryBackPressureClass.getDeclaredConstructors()) {
            if (constructor.getParameterCount() == 2){
                if(Configuration.class.isAssignableFrom(constructor.getParameterTypes()[0]) &&
                    Integer.class.isAssignableFrom(constructor.getParameterTypes()[1])){
                    constructorWithConfigurationAndBackpressureParams = constructor;
                }
            } else if (constructor.getParameterCount() == 1){
                if(Configuration.class.isAssignableFrom(constructor.getParameterTypes()[0])){
                    constructorWithConfigurationParam = constructor;
                }
            } else if(constructor.getParameterCount() == 0){
                parameterlessConstructor = constructor;
            }
        }

        try {

            if(constructorWithConfigurationAndBackpressureParams != null){
                queryBackPressure = (QueryBackPressure) constructorWithConfigurationAndBackpressureParams.newInstance(configuration, backPressureLimit);
            } else if(constructorWithConfigurationParam != null){
                queryBackPressure = (QueryBackPressure) constructorWithConfigurationParam.newInstance(configuration);
            } else if(parameterlessConstructor != null){
                queryBackPressure = (QueryBackPressure) parameterlessConstructor.newInstance();
            } else {
                throw new IllegalArgumentException(className + " has neither public constructor which accepts "
                    +Configuration.class.getName() + " and "+Integer.class.getName()+" nor constructor which accepts "+
                    Configuration.class.getName()+" parameter only, nor parameterless public constructor.");
            }

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Couldn't create a new instance of "+className +
                ". Please, check that the constructor which accepts "+Configuration.class.getName()
                +" is public or there is a public parameterless constructor. If the necessary public constructor exists, " +
                "please, check that invocation of this constructor doesn't throw an exception.", e);
        }

        log.info("Initiated custom query back pressure {}", className);

        return queryBackPressure;
    }
}
