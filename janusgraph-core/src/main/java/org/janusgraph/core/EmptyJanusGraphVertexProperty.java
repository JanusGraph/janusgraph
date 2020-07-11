// Copyright 2020 JanusGraph Authors
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

import java.util.Iterator;
import java.util.NoSuchElementException;

public final class EmptyJanusGraphVertexProperty<V> implements JanusGraphVertexProperty<V> {

    private static final EmptyJanusGraphVertexProperty INSTANCE = new EmptyJanusGraphVertexProperty();

    public static <U> EmptyJanusGraphVertexProperty<U> instance() {
        return INSTANCE;
    }

    @Override
    public JanusGraphVertex element() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        return null;
    }

    @Override
    public V value() throws NoSuchElementException {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public long longId() {
        return 0;
    }

    @Override
    public boolean hasId() {
        return false;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Property<V> property(String key, V value) {
        return null;
    }

    @Override
    public <V> V valueOrNull(PropertyKey key) {
        return null;
    }

    @Override
    public boolean isNew() {
        return false;
    }

    @Override
    public boolean isLoaded() {
        return false;
    }

    @Override
    public boolean isRemoved() {
        return false;
    }

    @Override
    public <V> V value(String key) {
        return null;
    }

    @Override
    public RelationType getType() {
        return null;
    }

    @Override
    public Direction direction(Vertex vertex) {
        return null;
    }

    @Override
    public boolean isIncidentOn(Vertex vertex) {
        return false;
    }

    @Override
    public boolean isLoop() {
        return false;
    }

    @Override
    public boolean isProperty() {
        return false;
    }

    @Override
    public boolean isEdge() {
        return false;
    }
}
