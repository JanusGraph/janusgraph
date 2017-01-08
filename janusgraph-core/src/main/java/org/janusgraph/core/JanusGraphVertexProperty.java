
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
