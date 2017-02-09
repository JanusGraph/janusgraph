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


package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * JanusGraphProperty is a {@link JanusGraphRelation} connecting a vertex to a value.
 * JanusGraphProperty extends {@link JanusGraphRelation}, with methods for retrieving the property's value and key.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see JanusGraphRelation
 * @see PropertyKey
 */
public interface JanusGraphVertexProperty<V> extends JanusGraphRelation, VertexProperty<V>, JanusGraphProperty<V> {

    /**
     * Returns the vertex on which this property is incident.
     *
     * @return The vertex of this property.
     */
    @Override
    public JanusGraphVertex element();

    @Override
    public default JanusGraphTransaction graph() {
        return element().graph();
    }

    @Override
    public default PropertyKey propertyKey() {
        return (PropertyKey)getType();
    }

}
