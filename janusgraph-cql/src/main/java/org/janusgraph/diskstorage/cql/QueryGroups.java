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

public class QueryGroups {

    private final List<DirectEqualityGroup> directEqualityGroups;
    private final List<SliceQuery> separateRangeQueries;

    public QueryGroups(List<DirectEqualityGroup> directEqualityGroups, List<SliceQuery> separateRangeQueries) {
        this.directEqualityGroups = directEqualityGroups;
        this.separateRangeQueries = separateRangeQueries;
    }

    /**
     * @return the groups of queries which each read a single column, every group read with one query per key or keys
     * group which asks for all of its columns, `column1 IN ?`
     */
    public List<DirectEqualityGroup> getDirectEqualityGroups() {
        return directEqualityGroups;
    }

    public List<SliceQuery> getSeparateRangeQueries() {
        return separateRangeQueries;
    }

    /**
     * Queries which each read a single column and share a limit, read together with that limit
     */
    public static class DirectEqualityGroup {

        private final int limit;
        private final List<SliceQuery> queries;

        public DirectEqualityGroup(int limit, List<SliceQuery> queries) {
            this.limit = limit;
            this.queries = queries;
        }

        public int getLimit() {
            return limit;
        }

        public List<SliceQuery> getQueries() {
            return queries;
        }
    }
}
