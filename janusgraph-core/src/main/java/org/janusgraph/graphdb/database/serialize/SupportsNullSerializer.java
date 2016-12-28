package org.janusgraph.graphdb.database.serialize;

import com.google.common.base.Preconditions;
import org.janusgraph.core.attribute.AttributeSerializer;

/**
 * Marker interface to indicate that a given serializer supports serializing
 * null values effectively.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SupportsNullSerializer {

}
