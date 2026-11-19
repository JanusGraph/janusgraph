// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.relations;

import com.google.common.base.Preconditions;

// An edge id that a mixed index returned together with the encoded sort key of the edge (see RelationSortKey), so
// that resolving it reads a single column. It hashes as the id, but equals only another EdgeIdWithSortKey of the same
// id, never the plain id: RelationIdentifier.equals only accepts relation identifiers, and an equality which held one
// way only would make membership depend on which side of the comparison the wrapper is. Callers that compare these ids
// with plain ones, as the intersection of the results of several indexes does, unwrap them first, see unwrap.
public final class EdgeIdWithSortKey {

    private final RelationIdentifier id;
    private final String sortKey;

    public EdgeIdWithSortKey(RelationIdentifier id, String sortKey) {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(sortKey);
        this.id = id;
        this.sortKey = sortKey;
    }

    public RelationIdentifier getId() {
        return id;
    }

    public String getSortKey() {
        return sortKey;
    }

    // The plain id of an element id returned by an index query
    public static Object unwrap(Object id) {
        return id instanceof EdgeIdWithSortKey ? ((EdgeIdWithSortKey) id).id : id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        return other instanceof EdgeIdWithSortKey && id.equals(((EdgeIdWithSortKey) other).id);
    }

    @Override
    public String toString() {
        return id.toString();
    }
}
