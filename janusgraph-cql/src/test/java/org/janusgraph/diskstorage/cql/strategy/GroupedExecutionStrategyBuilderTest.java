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

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.CQLStoreManager;
import org.janusgraph.diskstorage.cql.util.KeysGroup;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class GroupedExecutionStrategyBuilderTest {

    @Test
    public void shouldBuildTokenRangeAwareStrategy(){
        GroupedExecutionStrategy strategy = GroupedExecutionStrategyBuilder.build(null,
            Mockito.mock(CQLStoreManager.class), GroupedExecutionStrategyBuilder.TOKEN_RANGE_AWARE);
        Assertions.assertEquals(TokenRangeAwareGroupedExecutionStrategy.class, strategy.getClass());
    }

    @Test
    public void shouldBuildReplicasAwareStrategy(){
        CQLStoreManager storeManager = Mockito.mock(CQLStoreManager.class);
        Mockito.when(storeManager.getKeyspaceName()).thenReturn("testKeyspace");
        GroupedExecutionStrategy strategy = GroupedExecutionStrategyBuilder.build(null,
            storeManager, GroupedExecutionStrategyBuilder.REPLICAS_AWARE);
        Assertions.assertEquals(ReplicasAwareGroupedExecutionStrategy.class, strategy.getClass());
    }

    @Test
    public void shouldBuildSpecificStrategy(){
        GroupedExecutionStrategy strategy = GroupedExecutionStrategyBuilder.build(null,
            Mockito.mock(CQLStoreManager.class), TestGroupedExecutionStrategy.class.getName());
        Assertions.assertEquals(TestGroupedExecutionStrategy.class, strategy.getClass());
    }

    @Test
    public void shouldFailBuildCustomStrategyWhichThrowsException(){
        Assertions.assertThrows(IllegalStateException.class, () -> GroupedExecutionStrategyBuilder.build(null,
            Mockito.mock(CQLStoreManager.class), TestFailingGroupedExecutionStrategy.class.getName()));
    }

    @Test
    public void shouldFailBuildCustomStrategyWithoutNecessaryConstructor(){
        Assertions.assertThrows(IllegalArgumentException.class, () -> GroupedExecutionStrategyBuilder.build(null,
            Mockito.mock(CQLStoreManager.class), TestNoArgsGroupedExecutionStrategy.class.getName()));
    }

    @Test
    public void shouldFailBuildNonExistingClass(){
        Assertions.assertThrows(IllegalArgumentException.class, () -> GroupedExecutionStrategyBuilder.build(null,
            Mockito.mock(CQLStoreManager.class), "NonExistingStrategyImplementation"));
    }

    @Test
    public void shouldFailBuildIfNotImplementsProperInterface(){
        Assertions.assertThrows(IllegalArgumentException.class, () -> GroupedExecutionStrategyBuilder.build(null,
            Mockito.mock(CQLStoreManager.class), Integer.class.getName()));
    }

    private static class TestGroupedExecutionStrategy implements GroupedExecutionStrategy{

        public TestGroupedExecutionStrategy(Configuration configuration, CQLStoreManager session){
            // `configuration` and `session` is ignored
        }

        @Override
        public <R, Q> void execute(R futureResult,
                                   Q queries,
                                   List<StaticBuffer> keys,
                                   ResultFiller<R, Q, KeysGroup> withKeysGroupingFiller,
                                   ResultFiller<R, Q, List<StaticBuffer>> withoutKeysGroupingFiller,
                                   StoreTransaction txh,
                                   int keysGroupingLimit){
            // ignored
        }
    }

    public static class TestNoArgsGroupedExecutionStrategy implements GroupedExecutionStrategy{

        @Override
        public <R, Q> void execute(R futureResult, Q queries, List<StaticBuffer> keys, ResultFiller<R, Q, KeysGroup> withKeysGroupingFiller, ResultFiller<R, Q, List<StaticBuffer>> withoutKeysGroupingFiller, StoreTransaction txh, int keysGroupingLimit) {
            // ignored
        }
    }

    private static class TestFailingGroupedExecutionStrategy implements GroupedExecutionStrategy{

        public TestFailingGroupedExecutionStrategy(Configuration configuration, CQLStoreManager storeManager){
            throw new RuntimeException();
        }

        @Override
        public <R, Q> void execute(R futureResult,
                                   Q queries,
                                   List<StaticBuffer> keys,
                                   ResultFiller<R, Q, KeysGroup> withKeysGroupingFiller,
                                   ResultFiller<R, Q, List<StaticBuffer>> withoutKeysGroupingFiller,
                                   StoreTransaction txh,
                                   int keysGroupingLimit){
            // ignored
        }
    }
}
