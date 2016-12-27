package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.Namifiable;

/**
 * Marks any element that is part of a Titan Schema.
 * Titan Schema elements can be uniquely identified by their name.
 * <p/>
 * A Titan Schema element is either a {@link TitanSchemaType} or an index definition, i.e.
 * {@link TitanGraphIndex} or {@link RelationTypeIndex}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanSchemaElement extends Namifiable {

}
