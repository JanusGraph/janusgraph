
package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * JanusProperty is a {@link JanusRelation} connecting a vertex to a value.
 * JanusProperty extends {@link JanusRelation}, with methods for retrieving the property's value and key.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see JanusRelation
 * @see PropertyKey
 */
public interface JanusVertexProperty<V> extends JanusRelation, VertexProperty<V>, JanusProperty<V> {

    /**
     * Returns the vertex on which this property is incident.
     *
     * @return The vertex of this property.
     */
    @Override
    public JanusVertex element();

    @Override
    public default JanusTransaction graph() {
        return element().graph();
    }

    @Override
    public default PropertyKey propertyKey() {
        return (PropertyKey)getType();
    }

}
