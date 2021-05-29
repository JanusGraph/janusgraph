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

package org.janusgraph.graphdb.internal;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.types.IndexType;

/**
 * Internal Type interface adding methods that should only be used by JanusGraph
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalRelationType extends RelationType, InternalVertex {

    boolean isInvisibleType();

    long[] getSignature();

    long[] getSortKey();

    Order getSortOrder();

    Multiplicity multiplicity();

    ConsistencyModifier getConsistencyModifier();

    Integer getTTL();

    boolean isUnidirected(Direction dir);

    InternalRelationType getBaseType();

    Iterable<InternalRelationType> getRelationIndexes();

    SchemaStatus getStatus();

    Iterable<IndexType> getKeyIndexes();
}
