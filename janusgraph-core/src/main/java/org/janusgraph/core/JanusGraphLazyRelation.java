// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.transaction.RelationConstructor;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.util.Iterator;

public abstract class JanusGraphLazyRelation<V> implements InternalRelation  {

    private InternalRelation lazyLoadedRelation;
    private Entry dataEntry;
    private final InternalVertex vertex;
    private final StandardJanusGraphTx tx;
    private final InternalRelationType type;

    private final Object lockObject;

    public JanusGraphLazyRelation(InternalRelation janusGraphRelation,
                                  final InternalVertex vertex,
                                  final StandardJanusGraphTx tx,
                                  final InternalRelationType type) {
        this.lazyLoadedRelation = janusGraphRelation;
        this.dataEntry = null;
        this.vertex = vertex;
        this.tx = tx;
        this.type = type;
        this.lockObject = new Object();
    }

    public JanusGraphLazyRelation(Entry dataEntry,
                                  final InternalVertex vertex,
                                  final StandardJanusGraphTx tx,
                                  final InternalRelationType type) {
        this.lazyLoadedRelation = null;
        this.dataEntry = dataEntry;
        this.vertex = vertex;
        this.tx = tx;
        this.type = type;
        this.lockObject = new Object();
    }

    public InternalRelation loadValue() {
        if (this.lazyLoadedRelation == null) {
            synchronized (lockObject) {
                if (this.lazyLoadedRelation == null) {
                    if (tx.isClosed()) {
                        throw new IllegalStateException("Any lazy load operation is not supported when transaction is already closed.");
                    }
                    //parseHeaderOnly = false: it doesn't give much latency on read, but gives a good boost on writes
                    boolean parseHeaderOnly = tx.getConfiguration().isReadOnly();
                    this.lazyLoadedRelation = RelationConstructor.readRelation(this.vertex, this.dataEntry, parseHeaderOnly, this.tx);
                }
            }
        }
        return this.lazyLoadedRelation;
    }

    public V value() {
        assert this.isProperty();
        return ((Property<V>) this.loadValue()).value();
    }

    @Override
    public V value(String s) {
        return this.loadValue().value(s);
    }

    public boolean isSingle() {
        return this.type.multiplicity().getCardinality().equals(Cardinality.SINGLE);
    }

    @Override
    public RelationType getType() {
        return this.type;
    }

    @Override
    public Direction direction(Vertex vertex) {
        return this.loadValue().direction(vertex);
    }

    @Override
    public boolean isIncidentOn(Vertex vertex) {
        return this.loadValue().isIncidentOn(vertex);
    }

    @Override
    public boolean isLoop() {
        return this.loadValue().isLoop();
    }

    @Override
    public boolean isProperty() {
        return this.type.isPropertyKey();
    }

    @Override
    public boolean isEdge() {
        return this.type.isEdgeLabel();
    }

    @Override
    public String label() {
        return this.loadValue().label();
    }

    @Override
    public StandardJanusGraphTx tx() {
        return this.loadValue().tx();
    }

    @Override
    public JanusGraphTransaction graph() {
        return this.vertex.graph();
    }

    @Override
    public void setId(Object id) {
        this.loadValue().setId(id);
    }

    @Override
    public Object id() {
        return this.loadValue().id();
    }

    @Override
    public Object getCompareId() {
        return this.loadValue().getCompareId();
    }

    @Override
    public byte getLifeCycle() {
        return this.loadValue().getLifeCycle();
    }

    @Override
    public boolean isInvisible() {
        return this.type.isInvisibleType();
    }

    @Override
    public long longId() {
        return this.loadValue().longId();
    }

    @Override
    public boolean hasId() {
        return this.loadValue().hasId();
    }

    @Override
    public void remove() {
        this.loadValue().remove();
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(String... propertyKeys) {
        return this.loadValue().properties(propertyKeys);
    }

    @Override
    public <V> Property<V> property(String s, V v) {
        return this.loadValue().property(s, v);
    }

    @Override
    public <V> V valueOrNull(PropertyKey propertyKey) {
        return this.loadValue().valueOrNull(propertyKey);
    }

    @Override
    public boolean isNew() {
        return this.loadValue().isNew();
    }

    @Override
    public boolean isLoaded() {
        return this.loadValue().isLoaded();
    }

    @Override
    public boolean isRemoved() {
        return this.loadValue().isRemoved();
    }

    @Override
    public InternalRelation it() {
        InternalRelation loaded = this.loadValue();
        InternalRelation itRelation = loaded.it();
        if (itRelation == loaded) {
            return this;
        } else {
            return JanusGraphLazyRelationConstructor.create(itRelation, vertex, tx);
        }
    }

    @Override
    public InternalVertex getVertex(int pos) {
        return this.loadValue().getVertex(pos);
    }

    @Override
    public int getArity() {
        return this.loadValue().getArity();
    }

    @Override
    public int getLen() {
        return this.loadValue().getLen();
    }

    @Override
    public <O> O getValueDirect(PropertyKey key) {
        return this.loadValue().getValueDirect(key);
    }

    @Override
    public void setPropertyDirect(PropertyKey key, Object value) {
        this.loadValue().setPropertyDirect(key, value);
    }

    @Override
    public Iterable<PropertyKey> getPropertyKeysDirect() {
        return this.loadValue().getPropertyKeysDirect();
    }

    @Override
    public <O> O removePropertyDirect(PropertyKey key) {
        return this.loadValue().removePropertyDirect(key);
    }

    @Override
    public int hashCode() {
        return this.loadValue().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        InternalRelation thisRel = this.loadValue();
        if (other instanceof JanusGraphLazyRelation) {
            InternalRelation otherRel = ((JanusGraphLazyRelation<?>) other).loadValue();
            return thisRel.equals(otherRel);
        } else {
            return thisRel.equals(other);
        }
    }

    @Override
    public String toString() {
        return this.loadValue().toString();
    }
}
