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
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.types.system.ImplicitKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardVertexProperty extends AbstractVertexProperty implements StandardRelation, ReassignableRelation {

    private static final Map<PropertyKey, Object> EMPTY_PROPERTIES = Collections.emptyMap();

    private boolean isUpsert;
    private byte lifecycle;
    private long previousID = 0;
    private volatile Map<PropertyKey, Object> properties = EMPTY_PROPERTIES;

    public StandardVertexProperty(long id, PropertyKey type, InternalVertex vertex, Object value, byte lifecycle) {
        super(id, type, vertex, value);
        this.lifecycle = lifecycle;
    }

    /**
     * Mark this property as 'upsert', i.e. the old property value is not read from DB and marked as
     * deleted in the transaction
     * @param upsert
     */
    public void setUpsert(final boolean upsert) {
        isUpsert = upsert;
    }

    @Override
    public long getPreviousID() {
        return previousID;
    }

    @Override
    public void setPreviousID(long previousID) {
        Preconditions.checkArgument(previousID > 0);
        Preconditions.checkArgument(this.previousID == 0);
        this.previousID = previousID;
    }

    @Override
    public <O> O getValueDirect(PropertyKey key) {
        return (O) properties.get(key);
    }

    @Override
    public void setPropertyDirect(PropertyKey key, Object value) {
        Preconditions.checkArgument(!(key instanceof ImplicitKey), "Cannot use implicit type [%s] when setting property", key.name());
        if (properties == EMPTY_PROPERTIES) {
            if (tx().getConfiguration().isSingleThreaded()) {
                properties = new HashMap<>(5);
            } else {
                synchronized (this) {
                    if (properties == EMPTY_PROPERTIES) {
                        properties = Collections.synchronizedMap(new HashMap<>(5));
                    }
                }
            }
        }
        properties.put(key, value);
    }

    @Override
    public Iterable<PropertyKey> getPropertyKeysDirect() {
        return new ArrayList<>(properties.keySet());
    }

    @Override
    public <O> O removePropertyDirect(PropertyKey key) {
        if (!properties.isEmpty())
            return (O) properties.remove(key);
        else return null;
    }

    @Override
    public byte getLifeCycle() {
        return lifecycle;
    }

    @Override
    public synchronized void remove() {
        if (!ElementLifeCycle.isRemoved(lifecycle)) {
            tx().removeRelation(this);
            lifecycle = ElementLifeCycle.update(lifecycle, ElementLifeCycle.Event.REMOVED);
            if (isUpsert) {
                VertexProperty.Cardinality cardinality = ((PropertyKey) type).cardinality().convert();
                Consumer<JanusGraphVertexProperty> propertyRemover = JanusGraphVertexProperty.getRemover(cardinality, value());
                element().query().types(type.name()).properties().forEach(propertyRemover);
            }
        } //else throw InvalidElementException.removedException(this);
    }

}
