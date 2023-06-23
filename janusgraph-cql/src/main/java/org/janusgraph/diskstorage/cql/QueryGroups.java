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

package org.janusgraph.diskstorage.cql;

import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;

import java.util.List;
import java.util.Map;

public class QueryGroups {

    private final Map<Integer, List<SliceQuery>> directEqualityGroupedQueriesByLimit;
    private final List<SliceQuery> separateRangeQueries;

    public QueryGroups(Map<Integer, List<SliceQuery>> directEqualityGroupedQueriesByLimit, List<SliceQuery> separateRangeQueries) {
        this.directEqualityGroupedQueriesByLimit = directEqualityGroupedQueriesByLimit;
        this.separateRangeQueries = separateRangeQueries;
    }

    public Map<Integer, List<SliceQuery>> getDirectEqualityGroupedQueriesByLimit() {
        return directEqualityGroupedQueriesByLimit;
    }

    public List<SliceQuery> getSeparateRangeQueries() {
        return separateRangeQueries;
    }
}
