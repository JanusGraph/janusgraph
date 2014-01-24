package com.thinkaurelius.titan.graphdb.database.serialize;

/**
 * Marker interface to indicate that a given serializer preserves the natural
 * order of the elements (as given by its {@link Comparable} implementation)
 * in the binary representation.
 *
 * In other words, the byte order of the serialized representation is equal
 * to the natural order.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OrderPreservingSerializer {

}
