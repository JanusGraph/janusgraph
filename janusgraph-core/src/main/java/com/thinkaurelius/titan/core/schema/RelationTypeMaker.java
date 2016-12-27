package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;

/**
 * RelationTypeMaker is a factory for {@link com.thinkaurelius.titan.core.RelationType}s. RelationType can be configured to provide data verification,
 * better storage efficiency, and higher performance. The RelationType defines the schema for all {@link com.thinkaurelius.titan.core.TitanRelation}s
 * of that type.
 * <p/>
 * There are two kinds of RelationTypes: {@link com.thinkaurelius.titan.core.EdgeLabel} and {@link com.thinkaurelius.titan.core.PropertyKey} which
 * are defined via their builders {@link EdgeLabelMaker} and {@link PropertyKeyMaker} respectively. This interface just defines builder methods
 * common to both of them.
 * <p/>
 *
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see com.thinkaurelius.titan.core.RelationType
 */
public interface RelationTypeMaker {

    /**
     * Returns the name of this configured relation type.
     *
     * @return
     */
    public String getName();

    /**
     * Configures the signature of this relation type.
     * <p/>
     * Specifying the signature of a type tells the graph database to <i>expect</i> that relations of this type
     * always have or are likely to have an incident property or unidirected edge of the type included in the
     * signature. This allows the graph database to store such relations more compactly and retrieve them more quickly.
     * <br />
     * For instance, if all edges with label <i>friend</i> have a property with key <i>createdOn</i>, then specifying
     * (<i>createdOn</i>) as the signature for label <i>friend</i> allows friend edges to be stored more efficiently.
     * <br />
     * {@link RelationType}s used in the signature must be either property out-unique keys or out-unique unidirected edge labels.
     * <br />
     * The signature is empty by default.
     *
     * @param keys PropertyKey composing the signature for the configured relation type. The order is irrelevant.
     * @return this RelationTypeMaker
     */
    public RelationTypeMaker signature(PropertyKey... keys);

    /**
     * Builds the configured relation type
     *
     * @return the configured {@link RelationType}
     */
    public RelationType make();
}
