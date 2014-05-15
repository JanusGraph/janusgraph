package com.thinkaurelius.titan.core;

/**
 * TypeMaker is a factory for {@link RelationType}s. TitanTypes can be configured to provide data verification,
 * better storage efficiency, and higher performance. The TitanType defines the schema for all {@link TitanRelation}s
 * of that type.
 * </p>
 * All user defined types are configured using a TypeMaker instance returned by {@link com.thinkaurelius.titan.core.TitanTransaction#makePropertyKey(String)}}
 * or {@link TitanTransaction#makeEdgeLabel(String)} where the string parameter is the name of the type to be created.
 * Hence, types are defined within the context of a transaction like every other object in a TitanGraph. The configuration
 * options available when defining a type depend on whether its a key or a label. See {@link PropertyKeyMaker} and {@link EdgeLabelMaker} for more details.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see RelationType
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Type-Definition-Overview">Titan Type Wiki</a>
 */
public interface RelationTypeMaker {

    public String getName();

    public RelationType make();
}
