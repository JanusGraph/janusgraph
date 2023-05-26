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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class QueryBackPressureBuilderTest {

    @Test
    public void shouldBuildSemaphoreQueryBackPressure(){
        QueryBackPressure queryBackPressure = QueryBackPressureBuilder.build(null,
            QueryBackPressureBuilder.SEMAPHORE_QUERY_BACK_PRESSURE_CLASS, 10);
        Assertions.assertEquals(SemaphoreQueryBackPressure.class, queryBackPressure.getClass());
    }

    @Test
    public void shouldBuildSemaphoreProtectedReleaseQueryBackPressure(){
        QueryBackPressure queryBackPressure = QueryBackPressureBuilder.build(null,
            QueryBackPressureBuilder.SEMAPHORE_RELEASE_PROTECTED_QUERY_BACK_PRESSURE_CLASS, 10);
        Assertions.assertEquals(SemaphoreProtectedReleaseQueryBackPressure.class, queryBackPressure.getClass());
    }

    @Test
    public void shouldBuildPassAllQueryBackPressure(){
        QueryBackPressure queryBackPressure = QueryBackPressureBuilder.build(null,
            QueryBackPressureBuilder.PASS_ALL_QUERY_BACK_PRESSURE_CLASS, 10);
        Assertions.assertEquals(PassAllQueryBackPressure.class, queryBackPressure.getClass());
    }

    @Test
    public void shouldBuildCustomQueryBackPressureWithTwoArguments(){
        Configuration mockConfig = Mockito.mock(Configuration.class);
        int backPressureLimit = 10;
        CustomQueryBackPressureTwoArgs queryBackPressure = (CustomQueryBackPressureTwoArgs) QueryBackPressureBuilder.build(mockConfig,
            CustomQueryBackPressureTwoArgs.class.getName(), backPressureLimit);
        Assertions.assertEquals(mockConfig, queryBackPressure.getConfiguration());
        Assertions.assertEquals(backPressureLimit, queryBackPressure.getBackPressureLimit());
    }

    @Test
    public void shouldBuildCustomQueryBackPressureWithOneArgument(){
        Configuration mockConfig = Mockito.mock(Configuration.class);
        CustomQueryBackPressureOneArgs queryBackPressure = (CustomQueryBackPressureOneArgs) QueryBackPressureBuilder.build(mockConfig,
            CustomQueryBackPressureOneArgs.class.getName(), 10);
        Assertions.assertEquals(mockConfig, queryBackPressure.getConfiguration());
    }

    @Test
    public void shouldBuildCustomQueryBackPressureWithNoArguments(){
        Configuration mockConfig = Mockito.mock(Configuration.class);
        QueryBackPressure queryBackPressure = QueryBackPressureBuilder.build(mockConfig,
            CustomQueryBackPressureNoArgs.class.getName(), 10);
        Assertions.assertEquals(CustomQueryBackPressureNoArgs.class, queryBackPressure.getClass());
    }

    @Test
    public void shouldFailBuildCustomQueryBackPressureWithWrongArguments(){
        Assertions.assertThrows(IllegalArgumentException.class, () -> QueryBackPressureBuilder.build(null,
            CustomQueryBackPressureWrongArgs.class.getName(), 10));
    }

    @Test
    public void shouldFailBuildCustomQueryBackPressureWhichThrowsException(){
        Assertions.assertThrows(IllegalStateException.class, () -> QueryBackPressureBuilder.build(null,
            FailingCustomQueryBackPressure.class.getName(), 10));
    }

    @Test
    public void shouldFailBuildNonExistingCustomQueryBackPressure(){
        Assertions.assertThrows(IllegalArgumentException.class, () -> QueryBackPressureBuilder.build(null,
            "NonExistingCustomQueryBackPressureImplementation", 10));
    }

    @Test
    public void shouldFailBuildCustomQueryBackPressureWhichNotImplementsQueryBackPressure(){
        Assertions.assertThrows(IllegalArgumentException.class, () -> QueryBackPressureBuilder.build(null,
            Integer.class.getName(), 10));
    }

    public static class CustomQueryBackPressureTwoArgs extends PassAllQueryBackPressure{
        private final Configuration configuration;
        private final int backPressureLimit;
        public CustomQueryBackPressureTwoArgs(Configuration configuration, Integer backPressureLimit) {
            super();
            this.configuration = configuration;
            this.backPressureLimit = backPressureLimit;
        }
        public Configuration getConfiguration() {
            return configuration;
        }
        public int getBackPressureLimit() {
            return backPressureLimit;
        }
    }

    public static class CustomQueryBackPressureOneArgs extends PassAllQueryBackPressure{
        private final Configuration configuration;
        public CustomQueryBackPressureOneArgs(Configuration configuration) {
            super();
            this.configuration = configuration;
        }
        public Configuration getConfiguration() {
            return configuration;
        }
    }

    public static class CustomQueryBackPressureNoArgs extends PassAllQueryBackPressure{}

    public static class CustomQueryBackPressureWrongArgs extends PassAllQueryBackPressure{
        public CustomQueryBackPressureWrongArgs(String wrongArg) {}
    }

    public static class FailingCustomQueryBackPressure extends PassAllQueryBackPressure{
        public FailingCustomQueryBackPressure() {throw new RuntimeException();}
    }

}
