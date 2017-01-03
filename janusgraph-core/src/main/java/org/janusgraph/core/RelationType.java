package org.janusgraph.core;


import org.janusgraph.core.schema.JanusSchemaType;

/**
 * RelationType defines the schema for {@link JanusRelation}. RelationType can be configured through {@link org.janusgraph.core.schema.RelationTypeMaker} to
 * provide data verification, better storage efficiency, and higher retrieval performance.
 * <br />
 * Each {@link JanusRelation} has a unique type which defines many important characteristics of that relation.
 * <br />
 * RelationTypes are constructed through {@link org.janusgraph.core.schema.RelationTypeMaker} which is accessed in the context of a {@link JanusTransaction}
 * via {@link org.janusgraph.core.JanusTransaction#makePropertyKey(String)} for property keys or {@link org.janusgraph.core.JanusTransaction#makeEdgeLabel(String)}
 * for edge labels. Identical methods exist on {@link JanusGraph}.
 * Note, relation types will only be visible once the transaction in which they were created has been committed.
 * <br />
 * RelationType names must be unique in a graph database. Many methods allow the name of the type as an argument
 * instead of the actual type reference. That also means, that edge labels and property keys may not have the same name.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see JanusRelation
 * @see org.janusgraph.core.schema.RelationTypeMaker
 * @see <a href="http://s3.thinkaurelius.com/docs/janus/current/schema.html">"Schema and Data Modeling" manual chapter</a>
 */
public interface RelationType extends JanusVertex, JanusSchemaType {

    /**
     * Checks if this relation type is a property key
     *
     * @return true, if this relation type is a property key, else false.
     * @see PropertyKey
     */
    public boolean isPropertyKey();

    /**
     * Checks if this relation type is an edge label
     *
     * @return true, if this relation type is a edge label, else false.
     * @see EdgeLabel
     */
    public boolean isEdgeLabel();

}
