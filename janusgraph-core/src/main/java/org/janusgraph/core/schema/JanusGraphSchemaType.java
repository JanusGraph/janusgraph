package org.janusgraph.core.schema;

/**
 * A JanusGraphSchemaType is a {@link JanusGraphSchemaElement} that represents a label or key
 * used in the graph. As such, a schema type is either a {@link org.janusgraph.core.RelationType}
 * or a {@link org.janusgraph.core.VertexLabel}.
 * <p/>
 * JanusGraphSchemaTypes are a special {@link JanusGraphSchemaElement} in that they are referenced from the
 * main graph when creating vertices, edges, and properties.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusGraphSchemaType extends JanusGraphSchemaElement {
}
