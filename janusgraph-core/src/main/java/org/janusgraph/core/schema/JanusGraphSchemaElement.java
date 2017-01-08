package org.janusgraph.core.schema;

import org.janusgraph.core.Namifiable;

/**
 * Marks any element that is part of a JanusGraph Schema.
 * JanusGraph Schema elements can be uniquely identified by their name.
 * <p/>
 * A JanusGraph Schema element is either a {@link JanusGraphSchemaType} or an index definition, i.e.
 * {@link JanusGraphIndex} or {@link RelationTypeIndex}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusGraphSchemaElement extends Namifiable {

}
