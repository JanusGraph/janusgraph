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

import com.google.common.collect.Iterables;
import org.janusgraph.graphdb.query.Query;

import java.util.List;

/**
 * Represents a list of queries which should be performed for a specified key.
 *
 * @param <K> key
 */
public class KeyMultiQuery<K> {

    private final K key;

    private final List<SliceQuery> columnQueries;

    private final List<SliceQuery> columnSliceQueries;

    public KeyMultiQuery(K key, List<SliceQuery> columnQueries, List<SliceQuery> columnSliceQueries) {
        this.key = key;
        this.columnQueries = columnQueries;
        this.columnSliceQueries = columnSliceQueries;
    }

    public K getKey() {
        return key;
    }

    public List<SliceQuery> getColumnQueries() {
        return columnQueries;
    }

    public List<SliceQuery> getColumnSliceQueries() {
        return columnSliceQueries;
    }

    public Iterable<Query> getAllQueries() {
        return Iterables.concat(columnQueries, columnSliceQueries);
    }
}
