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

public class MultiKeysQueryGroups<K, Q extends Query> {

    private List<KeysQueriesGroup<K, Q>> queryGroups;

    private final MultiQueriesByKeysGroupsContext<K> multiQueryContext;

    public MultiKeysQueryGroups(List<KeysQueriesGroup<K, Q>> queryGroups, MultiQueriesByKeysGroupsContext<K> multiQueryContext) {
        this.queryGroups = queryGroups;
        this.multiQueryContext = multiQueryContext;
    }

    public List<KeysQueriesGroup<K, Q>> getQueryGroups() {
        return queryGroups;
    }

    public MultiQueriesByKeysGroupsContext<K> getMultiQueryContext() {
        return multiQueryContext;
    }

    public void setQueryGroups(List<KeysQueriesGroup<K, Q>> queryGroups) {
        this.queryGroups = queryGroups;
    }
}
