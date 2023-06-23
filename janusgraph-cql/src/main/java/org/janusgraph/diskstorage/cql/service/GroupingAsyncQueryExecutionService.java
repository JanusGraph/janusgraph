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

package org.janusgraph.diskstorage.cql.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.cql.CQLColValGetter;
import org.janusgraph.diskstorage.cql.CQLStoreManager;
import org.janusgraph.diskstorage.cql.QueryGroups;
import org.janusgraph.diskstorage.cql.function.slice.AsyncCQLMultiKeyMultiColumnFunction;
import org.janusgraph.diskstorage.cql.function.slice.AsyncCQLMultiKeySliceFunction;
import org.janusgraph.diskstorage.cql.function.slice.AsyncCQLSingleKeyMultiColumnFunction;
import org.janusgraph.diskstorage.cql.function.slice.AsyncCQLSingleKeySliceFunction;
import org.janusgraph.diskstorage.cql.query.MultiKeysMultiColumnQuery;
import org.janusgraph.diskstorage.cql.query.MultiKeysSingleSliceQuery;
import org.janusgraph.diskstorage.cql.query.SingleKeyMultiColumnQuery;
import org.janusgraph.diskstorage.cql.strategy.GroupedExecutionStrategy;
import org.janusgraph.diskstorage.cql.strategy.GroupedExecutionStrategyBuilder;
import org.janusgraph.diskstorage.cql.strategy.ResultFiller;
import org.janusgraph.diskstorage.cql.util.CQLSliceQueryUtil;
import org.janusgraph.diskstorage.cql.util.KeysGroup;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeysQueriesGroup;
import org.janusgraph.diskstorage.keycolumnvalue.MultiKeysQueryGroups;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.backpressure.QueryBackPressure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYS_GROUPING_ALLOWED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYS_GROUPING_CLASS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYS_GROUPING_LIMIT;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYS_GROUPING_MIN;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SLICE_GROUPING_ALLOWED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SLICE_GROUPING_LIMIT;
import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.COLUMN_BINDING;
import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.COLUMN_COLUMN_NAME;
import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.KEY_BINDING;
import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.KEY_COLUMN_NAME;
import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.LIMIT_BINDING;
import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.SLICE_END_BINDING;
import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.SLICE_START_BINDING;
import static org.janusgraph.diskstorage.cql.CQLKeyColumnValueStore.VALUE_COLUMN_NAME;

public class GroupingAsyncQueryExecutionService implements AsyncQueryExecutionService {

    private static final Logger log = LoggerFactory.getLogger(GroupingAsyncQueryExecutionService.class);

    private final ResultFiller<Map<StaticBuffer, CompletableFuture<EntryList>>, SliceQuery, KeysGroup> SINGLE_QUERY_WITH_KEYS_GROUPING_FILLER;
    private final ResultFiller<Map<StaticBuffer, CompletableFuture<EntryList>>, SliceQuery, List<StaticBuffer>> SINGLE_QUERY_WITHOUT_KEYS_GROUPING_FILLER;
    private final ResultFiller<Map<SliceQuery, Map<StaticBuffer, CompletableFuture<EntryList>>>, QueryGroups, KeysGroup> MULTI_QUERY_WITH_KEYS_GROUPING_FILLER;
    private final ResultFiller<Map<SliceQuery, Map<StaticBuffer, CompletableFuture<EntryList>>>, QueryGroups, List<StaticBuffer>> MULTI_QUERY_WITHOUT_KEYS_GROUPING_FILLER;

    private final AsyncCQLSingleKeySliceFunction cqlSingleKeySliceFunction;
    private final AsyncCQLSingleKeyMultiColumnFunction cqlSingleKeyMultiColumnFunction;
    private final AsyncCQLMultiKeySliceFunction cqlMultiKeySliceFunction;
    private final AsyncCQLMultiKeyMultiColumnFunction cqlMultiKeyMultiColumnFunction;
    private final boolean sliceGroupingAllowed;
    private final int sliceGroupingLimit;
    private final boolean keysGroupingAllowed;
    private final int keysGroupingLimit;
    private final int keysGroupingMin;
    private final GroupedExecutionStrategy groupedExecutionStrategy;

    public GroupingAsyncQueryExecutionService(Configuration configuration,
                                              final CQLStoreManager storeManager,
                                              String tableName,
                                              Function<Select, Select> addTTLFunction,
                                              Function<Select, Select> addTimestampFunction,
                                              CQLColValGetter singleKeyGetter,
                                              CQLColValGetter multiKeysGetter) {
        sliceGroupingLimit = getLimitOption(configuration, SLICE_GROUPING_LIMIT, 1);
        keysGroupingLimit = getLimitOption(configuration, KEYS_GROUPING_LIMIT, 1);
        keysGroupingMin = getLimitOption(configuration, KEYS_GROUPING_MIN, 2);
        keysGroupingAllowed = keysGroupingLimit > 1 && configuration.get(KEYS_GROUPING_ALLOWED);
        sliceGroupingAllowed = sliceGroupingLimit > 1 && configuration.get(SLICE_GROUPING_ALLOWED);
        String keyspaceName = storeManager.getKeyspaceName();
        CqlSession session = storeManager.getSession();
        ExecutorService executorService = storeManager.getExecutorService();
        QueryBackPressure queryBackPressure = storeManager.getQueriesBackPressure();

        // @formatter:off
        final Select getSliceSelect = selectFrom(keyspaceName, tableName)
            .column(COLUMN_COLUMN_NAME)
            .column(VALUE_COLUMN_NAME)
            .where(
                Relation.column(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING)),
                Relation.column(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING)),
                Relation.column(COLUMN_COLUMN_NAME).isLessThan(bindMarker(SLICE_END_BINDING))
            )
            .limit(bindMarker(LIMIT_BINDING));
        PreparedStatement getSlice = session.prepare(addTTLFunction.apply(addTimestampFunction.apply(getSliceSelect)).build());
        cqlSingleKeySliceFunction = new AsyncCQLSingleKeySliceFunction(session, getSlice, singleKeyGetter, executorService, queryBackPressure);

        if(sliceGroupingAllowed){
            // @formatter:off
            final Select getMultiColumnSelect = selectFrom(keyspaceName, tableName)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .where(
                    Relation.column(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING)),
                    Relation.column(COLUMN_COLUMN_NAME).in(bindMarker(COLUMN_BINDING))
                )
                .limit(bindMarker(LIMIT_BINDING));
            PreparedStatement getMultiColumn = session.prepare(addTTLFunction.apply(addTimestampFunction.apply(getMultiColumnSelect)).build());
            cqlSingleKeyMultiColumnFunction = new AsyncCQLSingleKeyMultiColumnFunction(session, getMultiColumn, singleKeyGetter, executorService, queryBackPressure);
        } else {
            cqlSingleKeyMultiColumnFunction = null;
        }

        if(keysGroupingAllowed){
            // @formatter:off
            final Select getMultiKeySliceSelect = selectFrom(keyspaceName, tableName)
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .where(
                    Relation.column(KEY_COLUMN_NAME).in(bindMarker(KEY_BINDING)),
                    Relation.column(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING)),
                    Relation.column(COLUMN_COLUMN_NAME).isLessThan(bindMarker(SLICE_END_BINDING))
                )
                .perPartitionLimit(bindMarker(LIMIT_BINDING));
            PreparedStatement getMultiKeySlice = session.prepare(addTTLFunction.apply(addTimestampFunction.apply(getMultiKeySliceSelect)).build());
            cqlMultiKeySliceFunction = new AsyncCQLMultiKeySliceFunction(session, getMultiKeySlice, multiKeysGetter, executorService, queryBackPressure);

            if(sliceGroupingAllowed){
                // @formatter:off
                final Select getMultiKeyMultiColumnSelect = selectFrom(keyspaceName, tableName)
                    .column(KEY_COLUMN_NAME)
                    .column(COLUMN_COLUMN_NAME)
                    .column(VALUE_COLUMN_NAME)
                    .where(
                        Relation.column(KEY_COLUMN_NAME).in(bindMarker(KEY_BINDING)),
                        Relation.column(COLUMN_COLUMN_NAME).in(bindMarker(COLUMN_BINDING))
                    )
                    .perPartitionLimit(bindMarker(LIMIT_BINDING));
                PreparedStatement getMultiKeyMultiColumn = session.prepare(addTTLFunction.apply(addTimestampFunction.apply(getMultiKeyMultiColumnSelect)).build());
                cqlMultiKeyMultiColumnFunction = new AsyncCQLMultiKeyMultiColumnFunction(session, getMultiKeyMultiColumn, multiKeysGetter, executorService, queryBackPressure);
            } else {
                cqlMultiKeyMultiColumnFunction = null;
            }

        } else {
            cqlMultiKeySliceFunction = null;
            cqlMultiKeyMultiColumnFunction = null;
        }

        SINGLE_QUERY_WITH_KEYS_GROUPING_FILLER = this::fillSingleQueryWithKeysGrouping;
        SINGLE_QUERY_WITHOUT_KEYS_GROUPING_FILLER = this::fillSingleQueryWithoutKeysGrouping;
        MULTI_QUERY_WITH_KEYS_GROUPING_FILLER = this::fillMultiQueryWithKeysGrouping;
        MULTI_QUERY_WITHOUT_KEYS_GROUPING_FILLER = this::fillMultiQueryWithoutKeysGrouping;

        groupedExecutionStrategy = GroupedExecutionStrategyBuilder.build(configuration, storeManager, configuration.get(KEYS_GROUPING_CLASS));
    }

    private static int getLimitOption(Configuration configuration, ConfigOption<Integer> limitOption, int minValue){
        int value = configuration.get(limitOption);
        if(value < minValue){
            log.warn("Configuration option `{}` is set to {}, but it should be {} or more. This configuration is going to be force-set to {}.", limitOption.toStringWithoutRoot(), value, minValue, minValue);
            return minValue;
        }
        return value;
    }

    @Override
    public CompletableFuture<EntryList> executeSingleKeySingleSlice(final KeySliceQuery query, final StoreTransaction txh) {
        return cqlSingleKeySliceFunction.execute(query, txh);
    }

    @Override
    public Map<StaticBuffer, CompletableFuture<EntryList>> executeMultiKeySingleSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) {
        Map<StaticBuffer, CompletableFuture<EntryList>> futureResult = new HashMap<>(keys.size());
        if(isKeysGroupingAllowed(keys)){
            groupedExecutionStrategy.execute(
                futureResult,
                query,
                keys,
                SINGLE_QUERY_WITH_KEYS_GROUPING_FILLER,
                SINGLE_QUERY_WITHOUT_KEYS_GROUPING_FILLER,
                txh,
                keysGroupingLimit
            );
        } else {
            fillSingleQueryWithoutKeysGrouping(
                futureResult,
                query,
                keys,
                txh
            );
        }
        return futureResult;
    }

    @Override
    public Map<SliceQuery, Map<StaticBuffer, CompletableFuture<EntryList>>> executeMultiKeyMultiSlice(final MultiKeysQueryGroups<StaticBuffer, SliceQuery> multiSliceQueriesForKeys, StoreTransaction txh) {
        final Map<SliceQuery, Map<StaticBuffer, CompletableFuture<EntryList>>> futureResult = new HashMap<>(multiSliceQueriesForKeys.getMultiQueryContext().getTotalAmountOfQueries());
        if(sliceGroupingAllowed){
            fillMultiSlicesWithQueryGrouping(futureResult, multiSliceQueriesForKeys, txh);
        } else {
            fillMultiSlicesWithoutQueryGrouping(futureResult, multiSliceQueriesForKeys, txh);
        }
        return futureResult;
    }

    private void fillMultiSlicesWithoutQueryGrouping(final Map<SliceQuery, Map<StaticBuffer, CompletableFuture<EntryList>>> futureResult,
                                                     final MultiKeysQueryGroups<StaticBuffer, SliceQuery> multiSliceQueriesForKeys,
                                                     final StoreTransaction txh){
        for(KeysQueriesGroup<StaticBuffer, SliceQuery> queryGroup : multiSliceQueriesForKeys.getQueryGroups()){
            List<StaticBuffer> keys = queryGroup.getKeysGroup();
            if(isKeysGroupingAllowed(keys)){
                for(SliceQuery query : queryGroup.getQueries()){
                    groupedExecutionStrategy.execute(
                        futureResult.computeIfAbsent(query, q -> new HashMap<>(keys.size())),
                        query,
                        keys,
                        SINGLE_QUERY_WITH_KEYS_GROUPING_FILLER,
                        SINGLE_QUERY_WITHOUT_KEYS_GROUPING_FILLER,
                        txh,
                        keysGroupingLimit
                    );
                }
            } else {
                for(SliceQuery query : queryGroup.getQueries()){
                    fillSingleQueryWithoutKeysGrouping(
                        futureResult.computeIfAbsent(query, q -> new HashMap<>(keys.size())),
                        query,
                        keys,
                        txh
                    );
                }
            }
        }
    }

    private void fillMultiSlicesWithQueryGrouping(final Map<SliceQuery, Map<StaticBuffer, CompletableFuture<EntryList>>> futureResult,
                                                  final MultiKeysQueryGroups<StaticBuffer, SliceQuery> multiSliceQueriesForKeys,
                                                  final StoreTransaction txh){

        for(KeysQueriesGroup<StaticBuffer, SliceQuery> queryGroup : multiSliceQueriesForKeys.getQueryGroups()){
            List<StaticBuffer> keys = queryGroup.getKeysGroup();
            QueryGroups queryGroups = CQLSliceQueryUtil.getQueriesGroupedByDirectEqualityQueries(queryGroup, multiSliceQueriesForKeys.getQueryGroups().size(), sliceGroupingLimit);
            if(isKeysGroupingAllowed(keys)){
                groupedExecutionStrategy.execute(futureResult, queryGroups, keys,
                    MULTI_QUERY_WITH_KEYS_GROUPING_FILLER, MULTI_QUERY_WITHOUT_KEYS_GROUPING_FILLER,
                    txh, keysGroupingLimit);
            } else {
                fillMultiQueryWithoutKeysGrouping(futureResult, queryGroups, keys, txh);
            }
        }
    }

    private void fillMultiQueryWithKeysGrouping(final Map<SliceQuery, Map<StaticBuffer, CompletableFuture<EntryList>>> futureResult,
                                                QueryGroups queryGroups,
                                                KeysGroup keysGroup,
                                                final StoreTransaction txh){

        // execute grouped queries
        for(Map.Entry<Integer, List<SliceQuery>> sliceQueriesGroup : queryGroups.getDirectEqualityGroupedQueriesByLimit().entrySet()){
            int limit = sliceQueriesGroup.getKey();
            List<ByteBuffer> queryStarts = new ArrayList<>(sliceQueriesGroup.getValue().size());
            Map<StaticBuffer, SliceQuery> columnToQueryMap = new HashMap<>(sliceQueriesGroup.getValue().size());
            for(SliceQuery sliceQuery : sliceQueriesGroup.getValue()){
                StaticBuffer column = sliceQuery.getSliceStart();
                queryStarts.add(column.asByteBuffer());
                columnToQueryMap.put(column, sliceQuery);
            }

            CompletableFuture<EntryList> multiKeyMultiColumnResult = cqlMultiKeyMultiColumnFunction.execute(new MultiKeysMultiColumnQuery(keysGroup.getRoutingToken(), keysGroup.getRawKeys(), queryStarts, limit), txh);
            Map<SliceQuery, Map<StaticBuffer, CompletableFuture<EntryList>>> partialResultToCompute = new HashMap<>(queryStarts.size());
            for(SliceQuery sliceQuery : sliceQueriesGroup.getValue()){
                Map<StaticBuffer, CompletableFuture<EntryList>> perKeyQueryPartialResult = new HashMap<>(keysGroup.size());
                partialResultToCompute.put(sliceQuery, perKeyQueryPartialResult);
                Map<StaticBuffer, CompletableFuture<EntryList>> perKeyQueryFutureResult = futureResult.computeIfAbsent(sliceQuery, q -> new HashMap<>(keysGroup.size()));
                for(StaticBuffer key : keysGroup.getKeys()){
                    CompletableFuture<EntryList> future = new CompletableFuture<>();
                    perKeyQueryFutureResult.put(key, future);
                    perKeyQueryPartialResult.put(key, future);
                }
            }
            multiKeyMultiColumnResult.whenComplete((entries, throwable) -> {
                if (throwable == null){

                    Map<SliceQuery, Map<StaticBuffer, EntryList>> returnedResult = new HashMap<>(partialResultToCompute.size());
                    for(Entry entry : entries){
                        StaticBuffer column = entry.getColumn();
                        StaticBuffer key = (StaticBuffer) entry.getMetaData().get(EntryMetaData.ROW_KEY);
                        assert key != null;
                        SliceQuery query = columnToQueryMap.get(column);
                        returnedResult.computeIfAbsent(query, q -> new HashMap<>(keysGroup.size())).computeIfAbsent(key, k -> new EntryArrayList()).add(entry);
                    }

                    for(Map.Entry<SliceQuery, Map<StaticBuffer, CompletableFuture<EntryList>>> futureResultEntry : partialResultToCompute.entrySet()){
                        SliceQuery query = futureResultEntry.getKey();
                        Map<StaticBuffer, CompletableFuture<EntryList>> futureKeysResults = futureResultEntry.getValue();
                        Map<StaticBuffer, EntryList> queryResults = returnedResult.get(query);
                        if(queryResults == null){
                            for(CompletableFuture<EntryList> keyResult : futureKeysResults.values()){
                                keyResult.complete(EntryList.EMPTY_LIST);
                            }
                        } else {
                            for(Map.Entry<StaticBuffer, CompletableFuture<EntryList>> futureKeyResultEntry : futureKeysResults.entrySet()){
                                futureKeyResultEntry.getValue().complete(queryResults.getOrDefault(futureKeyResultEntry.getKey(), EntryList.EMPTY_LIST));
                            }
                        }

                    }

                } else {
                    partialResultToCompute.values().forEach(keysMapToFail -> keysMapToFail.values().forEach(futureToFail -> futureToFail.completeExceptionally(throwable)));
                }
            });
        }

        // execute non-grouped queries
        for(SliceQuery separateQuery : queryGroups.getSeparateRangeQueries()){
            Map<StaticBuffer, CompletableFuture<EntryList>> perKeyQueryFutureResult = futureResult.computeIfAbsent(separateQuery, q -> new HashMap<>(keysGroup.size()));
            fillSingleQueryWithKeysGrouping(
                perKeyQueryFutureResult,
                separateQuery,
                keysGroup,
                txh
            );
        }
    }

    private void fillMultiQueryWithoutKeysGrouping(final Map<SliceQuery, Map<StaticBuffer, CompletableFuture<EntryList>>> futureResult,
                                                   QueryGroups queryGroups,
                                                   List<StaticBuffer> keys,
                                                   final StoreTransaction txh){

        // execute grouped queries
        for(Map.Entry<Integer, List<SliceQuery>> sliceQueriesGroup : queryGroups.getDirectEqualityGroupedQueriesByLimit().entrySet()){
            List<ByteBuffer> queryStarts = new ArrayList<>(sliceQueriesGroup.getValue().size());
            for(SliceQuery sliceQuery : sliceQueriesGroup.getValue()){
                queryStarts.add(sliceQuery.getSliceStart().asByteBuffer());
                futureResult.computeIfAbsent(sliceQuery, q -> new HashMap<>(keys.size()));
            }
            for(StaticBuffer key : keys){
                CompletableFuture<EntryList> multiColumnResult = cqlSingleKeyMultiColumnFunction.execute(new SingleKeyMultiColumnQuery(key.asByteBuffer(), queryStarts, sliceQueriesGroup.getKey()), txh);
                Map<SliceQuery, CompletableFuture<EntryList>> queryKeyFutureResult = new HashMap<>(sliceQueriesGroup.getValue().size());
                for(SliceQuery query : sliceQueriesGroup.getValue()){
                    CompletableFuture<EntryList> futureQueryKeyResult = new CompletableFuture<>();
                    queryKeyFutureResult.put(query, futureQueryKeyResult);
                    futureResult.get(query).put(key, futureQueryKeyResult);
                }
                multiColumnResult.whenComplete((entries, throwable) -> {
                    if (throwable == null){
                        Map<StaticBuffer, EntryList> columnToFilteredResult = new HashMap<>(sliceQueriesGroup.getValue().size());
                        entries.forEach(entry -> columnToFilteredResult.computeIfAbsent(entry.getColumn(), c -> new EntryArrayList()).add(entry));
                        queryKeyFutureResult.forEach((query, futureQueryResult) -> futureQueryResult.complete(columnToFilteredResult.getOrDefault(query.getSliceStart(), EntryList.EMPTY_LIST)));
                    } else {
                        queryKeyFutureResult.values().forEach(futureQueryResult -> futureQueryResult.completeExceptionally(throwable));
                    }
                });
            }
        }

        // execute non-grouped queries
        for(SliceQuery separateQuery : queryGroups.getSeparateRangeQueries()){
            Map<StaticBuffer, CompletableFuture<EntryList>> perKeyQueryFutureResult = futureResult.computeIfAbsent(separateQuery, q -> new HashMap<>(keys.size()));
            fillSingleQueryWithoutKeysGrouping(
                perKeyQueryFutureResult,
                separateQuery,
                keys,
                txh
            );
        }
    }

    private void fillSingleQueryWithKeysGrouping(final Map<StaticBuffer, CompletableFuture<EntryList>> futureQueryResult,
                                                 final SliceQuery query,
                                                 final KeysGroup keysGroup,
                                                 final StoreTransaction txh){

        CompletableFuture<EntryList> multiKeySingleSliceResult = cqlMultiKeySliceFunction.execute(new MultiKeysSingleSliceQuery(keysGroup.getRoutingToken(), keysGroup.getRawKeys(), query, query.getLimit()), txh);
        Map<StaticBuffer, CompletableFuture<EntryList>> perKeyQueryPartialResult = new HashMap<>(keysGroup.size());
        for(StaticBuffer key : keysGroup.getKeys()){
            CompletableFuture<EntryList> futureKeyResult = new CompletableFuture<>();
            futureQueryResult.put(key, futureKeyResult);
            perKeyQueryPartialResult.put(key, futureKeyResult);
        }

        multiKeySingleSliceResult.whenComplete((entries, throwable) -> {
            if (throwable == null){
                Map<StaticBuffer, EntryList> returnedResult = new HashMap<>(perKeyQueryPartialResult.size());
                for(Entry entry : entries){
                    StaticBuffer key = (StaticBuffer) entry.getMetaData().get(EntryMetaData.ROW_KEY);
                    assert key != null;
                    returnedResult.computeIfAbsent(key, k -> new EntryArrayList()).add(entry);
                }
                for(Map.Entry<StaticBuffer, CompletableFuture<EntryList>> futureKeyResultEntry : perKeyQueryPartialResult.entrySet()){
                    futureKeyResultEntry.getValue().complete(returnedResult.getOrDefault(futureKeyResultEntry.getKey(), EntryList.EMPTY_LIST));
                }
            } else {
                perKeyQueryPartialResult.values().forEach(futureToFail -> futureToFail.completeExceptionally(throwable));
            }
        });
    }

    private void fillSingleQueryWithoutKeysGrouping(final Map<StaticBuffer, CompletableFuture<EntryList>> futureQueryResult,
                                                    final SliceQuery query,
                                                    final List<StaticBuffer> keys,
                                                    final StoreTransaction txh){
        for(StaticBuffer key : keys){
            futureQueryResult.put(key, cqlSingleKeySliceFunction.execute(new KeySliceQuery(key, query), txh));
        }
    }

    private boolean isKeysGroupingAllowed(List<StaticBuffer> keys){
        return keysGroupingAllowed && keys.size() >= keysGroupingMin;
    }

}
