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

package org.janusgraph.diskstorage.keycolumnvalue;

import org.janusgraph.graphdb.query.Query;

import java.util.List;

public class KeysQueriesGroup<K, Q extends Query> {

    private final List<K> keysGroup;

    private List<Q> queries;

    public KeysQueriesGroup(List<K> keysGroup, List<Q> queries) {
        this.keysGroup = keysGroup;
        this.queries = queries;
    }

    public List<K> getKeysGroup() {
        return keysGroup;
    }

    public List<Q> getQueries() {
        return queries;
    }

    public void setQueries(List<Q> queries) {
        this.queries = queries;
    }
}
