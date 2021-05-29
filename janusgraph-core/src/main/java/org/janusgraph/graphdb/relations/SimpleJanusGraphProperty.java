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

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.internal.InternalRelation;

import java.util.NoSuchElementException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleJanusGraphProperty<V> implements JanusGraphProperty<V> {

    private final PropertyKey key;
    private final V value;
    private final InternalRelation relation;

    public SimpleJanusGraphProperty(InternalRelation relation, PropertyKey key, V value) {
        this.key = key;
        this.value = value;
        this.relation = relation;
    }

    @Override
    public PropertyKey propertyKey() {
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
    public JanusGraphElement element() {
        return relation;
    }

    @Override
    public void remove() {
        Preconditions.checkArgument(!relation.isRemoved(), "Cannot modified removed relation");
        relation.it().removePropertyDirect(key);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public boolean equals(Object oth) {
        return ElementHelper.areEqual(this, oth);
    }

}
