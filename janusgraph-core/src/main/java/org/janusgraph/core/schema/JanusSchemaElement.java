package org.janusgraph.core.schema;

import org.janusgraph.core.Namifiable;

/**
 * Marks any element that is part of a Janus Schema.
 * Janus Schema elements can be uniquely identified by their name.
 * <p/>
 * A Janus Schema element is either a {@link JanusSchemaType} or an index definition, i.e.
 * {@link JanusGraphIndex} or {@link RelationTypeIndex}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusSchemaElement extends Namifiable {

}
