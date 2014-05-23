package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.RelationType;

/**
 * TypeMaker is a factory for {@link com.thinkaurelius.titan.core.RelationType}s. TitanTypes can be configured to provide data verification,
 * better storage efficiency, and higher performance. The TitanType defines the schema for all {@link com.thinkaurelius.titan.core.TitanRelation}s
 * of that type.
 * </p>
 * All user defined types are configured using a TypeMaker instance returned by {@link com.thinkaurelius.titan.core.TitanTransaction#makePropertyKey(String)}}
 * or {@link com.thinkaurelius.titan.core.TitanTransaction#makeEdgeLabel(String)} where the string parameter is the name of the type to be created.
 * Hence, types are defined within the context of a transaction like every other object in a TitanGraph. The configuration
 * options available when defining a type depend on whether its a key or a label. See {@link PropertyKeyMaker} and {@link EdgeLabelMaker} for more details.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see com.thinkaurelius.titan.core.RelationType
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Type-Definition-Overview">Titan Type Wiki</a>
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
     * @param types TitanTypes composing the signature key. The order is irrelevant.
     * @return this relation type builder
     */
    public RelationTypeMaker signature(RelationType... types);


    public RelationType make();
}
