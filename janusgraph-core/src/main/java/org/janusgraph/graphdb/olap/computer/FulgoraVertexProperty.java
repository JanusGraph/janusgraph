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

package org.janusgraph.graphdb.olap.computer;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraVertexProperty<V> implements JanusGraphVertexProperty<V> {

    private final VertexMemoryHandler mixinParent;
    private final JanusGraphVertex vertex;
    private final String key;
    private final V value;
    private boolean isRemoved = false;

    public FulgoraVertexProperty(VertexMemoryHandler mixinParent, JanusGraphVertex vertex, String key, V value) {
        this.mixinParent = mixinParent;
        this.vertex = vertex;
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public JanusGraphVertex element() {
        return vertex;
    }

    @Override
    public void remove() {
        mixinParent.removeKey(key);
        isRemoved=true;
    }

    @Override
    public long longId() {
        throw new IllegalStateException("An id has not been set for this property");
    }

    @Override
    public boolean hasId() {
        return false;
    }

    @Override
    public <A> Property<A> property(String s, A v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> A valueOrNull(PropertyKey key) {
        return (A) property(key.name()).orElse(null);
    }

    @Override
    public boolean isNew() {
        return !isRemoved;
    }

    @Override
    public boolean isLoaded() {
        return false;
    }

    @Override
    public boolean isRemoved() {
        return isRemoved;
    }

    @Override
    public <A> A value(String key) {
        throw Property.Exceptions.propertyDoesNotExist(this,key);
    }

    @Override
    public RelationType getType() { throw new UnsupportedOperationException(); }

    @Override
    public Direction direction(Vertex vertex) {
        if (isIncidentOn(vertex)) return Direction.OUT;
        throw new IllegalArgumentException("Property is not incident on vertex");
    }

    @Override
    public boolean isIncidentOn(Vertex vertex) {
        return this.vertex.equals(vertex);
    }

    @Override
    public boolean isLoop() {
        return false;
    }

    @Override
    public boolean isProperty() {
        return true;
    }

    @Override
    public boolean isEdge() {
        return false;
    }

    @Override
    public <A> Iterator<Property<A>> properties(String... propertyKeys) {
        return Collections.emptyIterator();
    }
}
