package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;


/**
 * Allows custom serializer definitions for attribute values.
 * <p/>
 * For most data types (i.e. classes) used with properties, using the default serializer when registering the type with the
 * graph database will be sufficient and efficient in practice. However, for certain data types, it can be more
 * efficient to provide custom serializers implementing this interface.
 * Such custom serializers are registered in the configuration file by specifying their path and loaded when
 * the database is initialized. Hence, the serializer must be on the classpath.
 * <br />
 * <p/>
 * When a {@link TitanKey} is defined using a data type specified via {@link TypeMaker} for which a custom serializer
 * is configured, then it will use this custom serializer for persistence operations. For more information on how to
 * configure a custom serializer, refer to the
 * <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
 *
 * @param <V> Type of the attribute associated with the AttributeSerializer
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see TypeMaker
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
 */
public interface AttributeSerializer<V> extends AttributeHandler<V> {

    /**
     * Reads an attribute from the given ReadBuffer.
     * <p/>
     * It is expected that this read operation adjusts the position in the ReadBuffer to after the attribute value.
     *
     * @param buffer ReadBuffer to read attribute from
     * @return Read attribute
     */
    public V read(ScanBuffer buffer);

    /**
     * Writes the attribute value to the given WriteBuffer.
     * <p/>
     * It is expected that this write operation adjusts the position in the WriteBuffer to after the attribute value.
     *
     * @param buffer    WriteBuffer to write attribute to
     * @param attribute Attribute to write to WriteBuffer
     */
    public void writeObjectData(WriteBuffer buffer, V attribute);

}
