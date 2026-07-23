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

package org.janusgraph.graphdb.database.index;

import org.janusgraph.graphdb.internal.ElementCategory;

import java.util.Objects;

/**
 * A normalized, value-free description of a single graph element that changed and therefore may need its mixed-index
 * documents refreshed. CDC capture sources (e.g. the Debezium Cassandra decoder) translate raw change events into these,
 * and {@link MixedIndexUpdateApplier} reindexes the referenced element from its current graph state.
 *
 * <p>It deliberately carries no field values: the apply step always reads the element's current state from the graph,
 * which makes index updates idempotent and order-independent.</p>
 *
 * <p>{@code elementId} is a {@link Long} for {@link ElementCategory#VERTEX} and a
 * {@link org.janusgraph.graphdb.relations.RelationIdentifier} for {@link ElementCategory#EDGE} /
 * {@link ElementCategory#PROPERTY}, matching {@link ElementCategory#retrieve(Object, org.janusgraph.core.JanusGraphTransaction)}.</p>
 */
public final class CdcElementChange {

    private final ElementCategory category;
    private final Object elementId;

    public CdcElementChange(ElementCategory category, Object elementId) {
        this.category = Objects.requireNonNull(category, "category");
        this.elementId = Objects.requireNonNull(elementId, "elementId");
    }

    public ElementCategory getCategory() {
        return category;
    }

    public Object getElementId() {
        return elementId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CdcElementChange)) return false;
        CdcElementChange that = (CdcElementChange) o;
        return category == that.category && elementId.equals(that.elementId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, elementId);
    }

    @Override
    public String toString() {
        return "CdcElementChange{" + category + ":" + elementId + '}';
    }
}
