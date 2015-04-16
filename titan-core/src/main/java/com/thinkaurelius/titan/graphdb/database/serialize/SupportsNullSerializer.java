package com.thinkaurelius.titan.graphdb.database.serialize;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.AttributeSerializer;

/**
 * Marker interface to indicate that a given serializer supports serializing
 * null values effectively.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SupportsNullSerializer {

}
