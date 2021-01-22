// Copyright 2017 JanusGraph Authors
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

import org.janusgraph.diskstorage.StaticBuffer;

import java.util.List;
import java.util.Objects;

/**
 * Queries for a list of slices of data, each identified by a start points (inclusive) and end points (exclusive).
 * Returns all {@link StaticBuffer}s that lie in this ranges up to the given limit.
 * <p>
 * If a MultiSlicesQuery is marked <i>static</i> it is expected that the result set does not change.
 *
 * @author Sergii Karpenko (sergiy.karpenko@gmail.com)
 */

public class MultiSlicesQuery {

    private final List<SliceQuery> queries;

    public MultiSlicesQuery(List<SliceQuery> queries) {
        this.queries = queries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiSlicesQuery that = (MultiSlicesQuery) o;
        return Objects.equals(queries, that.queries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queries);
    }
}
