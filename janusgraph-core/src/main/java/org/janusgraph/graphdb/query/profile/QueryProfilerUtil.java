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

package org.janusgraph.graphdb.query.profile;

import org.janusgraph.diskstorage.keycolumnvalue.KeysQueriesGroup;
import org.janusgraph.diskstorage.keycolumnvalue.MultiKeysQueryGroups;
import org.janusgraph.graphdb.query.Query;

import java.util.ArrayList;
import java.util.List;

public class QueryProfilerUtil {

    private QueryProfilerUtil(){}

    public static <Q extends Query> void setMultiSliceQueryAnnotations(QueryProfiler profiler, MultiKeysQueryGroups<Object, Q> multiSliceQueries){
        if(profiler == QueryProfiler.NO_OP){
            return;
        }
        profiler.setAnnotation(QueryProfiler.MULTI_SLICES_ANNOTATION, true);
        List<Q> allQueries = new ArrayList<>();
        ArrayList<Integer> allLimits = null;
        boolean hasLimit = false;
        int queriesCount = 0;
        for(KeysQueriesGroup<Object, Q> groupedQueries : multiSliceQueries.getQueryGroups()){
            for(Q query : groupedQueries.getQueries()){
                allQueries.add(query);
                if(hasLimit){
                    allLimits.add(query.hasLimit() ? query.getLimit() : -1);
                } else if(query.hasLimit()){
                    hasLimit = true;
                    allLimits = new ArrayList<>();
                    allLimits.ensureCapacity(queriesCount+1);
                    for(int i=0;i<queriesCount;i++){
                        allLimits.add(-1);
                    }
                    allLimits.add(query.getLimit());
                }
                ++queriesCount;
            }
        }
        profiler.setAnnotation(QueryProfiler.QUERIES_ANNOTATION, allQueries);
        profiler.setAnnotation(QueryProfiler.QUERIES_AMOUNT_ANNOTATION, queriesCount);
        if(hasLimit){
            profiler.setAnnotation(QueryProfiler.QUERY_LIMITS_ANNOTATION,allLimits);
        }
    }

}
