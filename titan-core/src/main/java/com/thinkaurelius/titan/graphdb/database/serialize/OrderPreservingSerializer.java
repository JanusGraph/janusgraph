package com.thinkaurelius.titan.graphdb.database.serialize;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

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
public interface OrderPreservingSerializer<V> extends AttributeSerializer<V> {


    /**
     * Reads an attribute from the given ReadBuffer assume it was written in byte order.
     * <p/>
     * It is expected that this read operation adjusts the position in the ReadBuffer to after the attribute value.
     *
     * @param buffer ReadBuffer to read attribute from
     * @return Read attribute
     */
    public V readByteOrder(ScanBuffer buffer);

    /**
     * Writes the attribute value to the given WriteBuffer such that the byte order of the result is equal to the
     * natural order of the attribute.
     * <p/>
     * It is expected that this write operation adjusts the position in the WriteBuffer to after the attribute value.
     *
     * @param buffer    WriteBuffer to write attribute to
     * @param attribute Attribute to write to WriteBuffer
     */
    public void writeByteOrder(WriteBuffer buffer, V attribute);

}
