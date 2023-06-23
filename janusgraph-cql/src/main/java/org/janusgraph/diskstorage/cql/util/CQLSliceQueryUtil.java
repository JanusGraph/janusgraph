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

package org.janusgraph.diskstorage.cql.util;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.cql.QueryGroups;
import org.janusgraph.diskstorage.keycolumnvalue.KeysQueriesGroup;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CQLSliceQueryUtil {

    private CQLSliceQueryUtil(){}

    public static QueryGroups getQueriesGroupedByDirectEqualityQueries(KeysQueriesGroup<StaticBuffer, SliceQuery> queryGroup, int totalQueryGroupsSize, int sliceGroupingLimit){
        Map<Integer, List<SliceQuery>> directEqualityGroupedQueriesByLimit = new HashMap<>(queryGroup.getQueries().size());
        List<SliceQuery> separateRangeQueries = new ArrayList<>(queryGroup.getQueries().size());
        for(SliceQuery query : queryGroup.getQueries()){
            if(query.isDirectColumnByStartOnlyAllowed()){
                List<SliceQuery> directEqualityQueries = directEqualityGroupedQueriesByLimit.get(query.getLimit());
                if(directEqualityQueries == null){
                    directEqualityQueries = new ArrayList<>(totalQueryGroupsSize);
                    directEqualityQueries.add(query);
                    directEqualityGroupedQueriesByLimit.put(query.getLimit(), directEqualityQueries);
                } else if(directEqualityQueries.size() < sliceGroupingLimit && (!query.hasLimit() || directEqualityQueries.size() < query.getLimit())){
                    // We cannot group more than `query.getLimit()` queries together.
                    // Even so it seems that it makes sense to group them together because we don't need
                    // more column values than limit - we are still obliged to compute the result because
                    // any separate SliceQuery can be cached into a tx-cache or db-cache with incomplete result
                    // which may result in the wrong results for future calls of the cached Slice queries.
                    // Thus, we add a query into the group only if it doesn't have any limit set OR the total
                    // amount of grouped together direct equality queries is <= than the limit requested.
                    // I.e. in other words, we must ensure that the limit won't influence the final result.
                    directEqualityQueries.add(query);
                } else {
                    // In this case we couldn't group a query. Thus, we should execute this query separately.
                    separateRangeQueries.add(query);
                }
            } else {
                // We cannot group range queries together. Thus, they are executed separately.
                separateRangeQueries.add(query);
            }
        }

        return new QueryGroups(directEqualityGroupedQueriesByLimit, separateRangeQueries);
    }

    public static TokenRange findTokenRange(Token token, Collection<TokenRange> tokenRanges){
        for(TokenRange tokenRange : tokenRanges){
            if(tokenRange.contains(token)){
                return tokenRange;
            }
        }
        return null;
    }

}
