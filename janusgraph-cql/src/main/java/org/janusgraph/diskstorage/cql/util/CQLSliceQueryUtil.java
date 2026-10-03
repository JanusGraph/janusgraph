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

    /**
     * Groups the queries which read a single column, so that each group is read with one query per key or keys group.
     * A group holds at most `sliceGroupingLimit` queries, and no more than the limit they share: the query of a group
     * asks for that many columns, and a query whose column was cut off would be cached with an incomplete result.
     * A full group is followed by another one for the same limit. Queries which read a range of columns are read
     * separately.
     */
    public static QueryGroups getQueriesGroupedByDirectEqualityQueries(KeysQueriesGroup<StaticBuffer, SliceQuery> queryGroup, int sliceGroupingLimit){
        final List<QueryGroups.DirectEqualityGroup> directEqualityGroups = new ArrayList<>();
        //The group of each limit which still takes queries
        final Map<Integer, List<SliceQuery>> openGroupsByLimit = new HashMap<>();
        final List<SliceQuery> separateRangeQueries = new ArrayList<>(queryGroup.getQueries().size());
        for(SliceQuery query : queryGroup.getQueries()){
            if(query.isDirectColumnByStartOnlyAllowed()){
                final int groupSize = query.hasLimit() ? Math.max(1, Math.min(sliceGroupingLimit, query.getLimit())) : sliceGroupingLimit;
                List<SliceQuery> directEqualityQueries = openGroupsByLimit.get(query.getLimit());
                if(directEqualityQueries == null || directEqualityQueries.size() >= groupSize){
                    directEqualityQueries = new ArrayList<>(Math.min(groupSize, queryGroup.getQueries().size()));
                    openGroupsByLimit.put(query.getLimit(), directEqualityQueries);
                    directEqualityGroups.add(new QueryGroups.DirectEqualityGroup(query.getLimit(), directEqualityQueries));
                }
                directEqualityQueries.add(query);
            } else {
                // We cannot group range queries together. Thus, they are executed separately.
                separateRangeQueries.add(query);
            }
        }

        return new QueryGroups(directEqualityGroups, separateRangeQueries);
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
