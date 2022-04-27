// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.graphdb.database;

import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;

public class IndexRecordEntry {
    final long relationId;
    final Object value;
    final PropertyKey key;

    public IndexRecordEntry(long relationId, Object value, PropertyKey key) {
        this.relationId = relationId;
        this.value = value;
        this.key = key;
    }

    public IndexRecordEntry(JanusGraphVertexProperty property) {
        this(property.longId(),property.value(),property.propertyKey());
    }

    public long getRelationId() {
        return relationId;
    }

    public Object getValue() {
        return value;
    }

    public PropertyKey getKey() {
        return key;
    }
}
