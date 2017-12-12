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

package org.janusgraph.graphdb.relations;

import org.janusgraph.core.*;
import org.janusgraph.graphdb.internal.AbstractElement;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.stream.Stream;

public abstract class AbstractTypedRelation extends AbstractElement implements InternalRelation {

    protected final InternalRelationType type;

    public AbstractTypedRelation(final long id, final RelationType type) {
        super(id);
        assert type != null && type instanceof InternalRelationType;
        this.type = (InternalRelationType) type;
    }

    @Override
    public InternalRelation it() {
        InternalVertex v = getVertex(0);
        if (v == v.it())
            return this;

        InternalRelation next = (InternalRelation) RelationIdentifier.get(this).findRelation(tx());
        if (next == null)
            throw InvalidElementException.removedException(this);

        return next;
    }

    @Override
    public final StandardJanusGraphTx tx() {
        return getVertex(0).tx();
    }

    /**
     * Cannot currently throw exception when removed since internal logic relies on access to the edge
     * beyond its removal. TODO: reconcile with access validation logic
     */
    protected final void verifyAccess() { }

	/* ---------------------------------------------------------------
	 * Immutable Aspects of Relation
	 * ---------------------------------------------------------------
	 */

    @Override
    public Direction direction(Vertex vertex) {
        for (int i=0;i<getArity();i++) {
            if (it().getVertex(i).equals(vertex)) return EdgeDirection.fromPosition(i);
        }
        throw new IllegalArgumentException("Relation is not incident on vertex");
    }

    @Override
    public boolean isIncidentOn(Vertex vertex) {
        for (int i=0;i<getArity();i++) {
            if (it().getVertex(i).equals(vertex)) return true;
        }
        return false;
    }

    @Override
    public boolean isInvisible() {
        return type.isInvisibleType();
    }

    @Override
    public boolean isLoop() {
        return getArity()==2 && getVertex(0).equals(getVertex(1));
    }

    @Override
    public RelationType getType() {
        return type;
    }

    @Override
    public RelationIdentifier id() {
        return RelationIdentifier.get(this);
    }

    /* ---------------------------------------------------------------
	 * Mutable Aspects of Relation
	 * ---------------------------------------------------------------
	 */

    @Override
    public <V> Property<V> property(final String key, final V value) {
        verifyAccess();

        PropertyKey propertyKey = tx().getOrCreatePropertyKey(key);
        Object normalizedValue = tx().verifyAttribute(propertyKey,value);
        it().setPropertyDirect(propertyKey,normalizedValue);
        return new SimpleJanusGraphProperty<>(this, propertyKey, value);
    }

    @Override
    public <O> O valueOrNull(PropertyKey key) {
        verifyAccess();
        if (key instanceof ImplicitKey) return ((ImplicitKey)key).computeProperty(this);
        return it().getValueDirect(key);
    }

    @Override
    public <O> O value(String key) {
        verifyAccess();
        O val = valueInternal(tx().getPropertyKey(key));
        if (val==null) throw Property.Exceptions.propertyDoesNotExist(this,key);
        return val;
    }

    private <O> O valueInternal(PropertyKey type) {
        if (type==null) {
            return null;
        }
        return valueOrNull(type);
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... keyNames) {
        verifyAccess();

        Stream<PropertyKey> keys;

        if (keyNames==null || keyNames.length==0) {
            keys = IteratorUtils.stream(it().getPropertyKeysDirect().iterator());
        } else {
            keys = Stream.of(keyNames)
                    .map(s -> tx().getPropertyKey(s)).filter(rt -> rt != null && getValueDirect(rt)!=null);
        }
        return keys.map( rt -> (Property<V>)new SimpleJanusGraphProperty<V>(this,rt,valueInternal(rt))).iterator();
    }

    /* ---------------------------------------------------------------
	 * Blueprints Iterators
	 * ---------------------------------------------------------------
	 */

//    @Override
//    public Iterator<Vertex> vertexIterator(Direction direction) {
//        verifyAccess();
//
//        List<Vertex> vertices;
//        if (direction==Direction.BOTH) {
//            vertices = ImmutableList.of((Vertex)getVertex(0),getVertex(1));
//        } else {
//            vertices = ImmutableList.of((Vertex)getVertex(EdgeDirection.position(direction)));
//        }
//        return vertices.iterator();
//    }
//
//    @Override
//    public <V> Iterator<Property<V>> propertyIterator(String... keyNames) {
//        verifyAccess();
//
//        Stream<RelationType> keys;
//
//        if (keyNames==null || keyNames.length==0) {
//            keys = IteratorUtils.stream(it().getPropertyKeysDirect());
//        } else {
//            keys = Stream.of(keyNames)
//                    .map(s -> tx().getRelationType(s)).filter(rt -> rt != null && getValueDirect(rt)!=null);
//        }
//        return keys.map( rt -> (Property<V>)new SimpleJanusGraphProperty<V>(this,rt,valueInternal(rt))).iterator();
//    }


}
