package org.janusgraph.core;


import org.janusgraph.core.schema.TitanSchemaType;

/**
 * RelationType defines the schema for {@link TitanRelation}. RelationType can be configured through {@link org.janusgraph.core.schema.RelationTypeMaker} to
 * provide data verification, better storage efficiency, and higher retrieval performance.
 * <br />
 * Each {@link TitanRelation} has a unique type which defines many important characteristics of that relation.
 * <br />
 * RelationTypes are constructed through {@link org.janusgraph.core.schema.RelationTypeMaker} which is accessed in the context of a {@link TitanTransaction}
 * via {@link org.janusgraph.core.TitanTransaction#makePropertyKey(String)} for property keys or {@link org.janusgraph.core.TitanTransaction#makeEdgeLabel(String)}
 * for edge labels. Identical methods exist on {@link TitanGraph}.
 * Note, relation types will only be visible once the transaction in which they were created has been committed.
 * <br />
 * RelationType names must be unique in a graph database. Many methods allow the name of the type as an argument
 * instead of the actual type reference. That also means, that edge labels and property keys may not have the same name.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 * @see TitanRelation
 * @see org.janusgraph.core.schema.RelationTypeMaker
 * @see <a href="http://s3.thinkaurelius.com/docs/titan/current/schema.html">"Schema and Data Modeling" manual chapter</a>
 */
public interface RelationType extends TitanVertex, TitanSchemaType {

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
