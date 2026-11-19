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

package org.janusgraph.diskstorage.indexing;

import com.google.common.base.Preconditions;

import java.util.Objects;

// A document matched by IndexProvider.queryHits: its id and, for an edge document, the value of
// IndexProvider.EDGE_SORT_KEY_FIELD, or null when the document has none.
public final class IndexHit {

    private final String id;
    private final String sortKey;

    public IndexHit(String id, String sortKey) {
        Preconditions.checkNotNull(id);
        this.id = id;
        this.sortKey = sortKey;
    }

    public String getId() {
        return id;
    }

    public String getSortKey() {
        return sortKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexHit)) return false;
        final IndexHit other = (IndexHit) o;
        return id.equals(other.id) && Objects.equals(sortKey, other.sortKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, sortKey);
    }

    @Override
    public String toString() {
        return sortKey == null ? id : id + "[" + sortKey + "]";
    }
}
