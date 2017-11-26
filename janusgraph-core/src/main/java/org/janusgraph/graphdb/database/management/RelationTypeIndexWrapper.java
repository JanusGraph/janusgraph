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

package org.janusgraph.graphdb.database.management;

import com.google.common.base.Preconditions;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;

import lombok.Getter;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationTypeIndexWrapper implements RelationTypeIndex {

    public static final char RELATION_INDEX_SEPARATOR = ':';

    @Getter
    private final InternalRelationType wrappedType;

    public RelationTypeIndexWrapper(InternalRelationType type) {
        Preconditions.checkArgument(type != null && type.getBaseType() != null);
        this.wrappedType = type;
    }

    @Override
    public RelationType getType() {
        return wrappedType.getBaseType();
    }

    @Override
    public String name() {
        String typeName = wrappedType.name();
        int index = typeName.lastIndexOf(RELATION_INDEX_SEPARATOR);
        Preconditions.checkArgument(index > 0 && index < typeName.length() - 1, "Invalid name encountered: %s", typeName);
        return typeName.substring(index + 1, typeName.length());
    }

    @Override
    public Order getSortOrder() {
        return wrappedType.getSortOrder().getTP();

    }

    @Override
    public RelationType[] getSortKey() {
        StandardJanusGraphTx tx = wrappedType.tx();
        long[] ids = wrappedType.getSortKey();
        RelationType[] keys = new RelationType[ids.length];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = tx.getExistingRelationType(ids[i]);
        }
        return keys;
    }

    @Override
    public Direction getDirection() {
        if (wrappedType.isUnidirected(Direction.BOTH)) return Direction.BOTH;
        else if (wrappedType.isUnidirected(Direction.OUT)) return Direction.OUT;
        else if (wrappedType.isUnidirected(Direction.IN)) return Direction.IN;
        throw new AssertionError();
    }

    @Override
    public int hashCode() {
        return wrappedType.hashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (oth == null) return false;
        else if (oth == this) return true;
        else if (!getClass().isInstance(oth)) return false;
        return wrappedType.equals(((RelationTypeIndexWrapper) oth).wrappedType);
    }

    @Override
    public SchemaStatus getIndexStatus() {
        return wrappedType.getStatus();
    }

    @Override
    public String toString() {
        return name();
    }
}
