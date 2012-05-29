package com.thinkaurelius.titan.core;

import java.nio.ByteBuffer;

/**
 * Allows custom serializer definitions for attribute values.
 *
 * For most data types (i.e. classes) used with properties, using the default serializer when registering the type with the
 * graph database will be sufficient and efficient in practice. However, for certain data types, it can be more
 * efficient to provide custom serializers implementing this interface.
 * Such custom serializers are registered in the configuration file by specifying their path and loaded when 
 * the database is initialized. Hence, the serializer must be on the classpath.
 * <br />
 *
 * When a {@link TitanKey} is defined using a data type specified via {@link TypeMaker} for which a custom serializer
 * is configured, then it will use this custom serializer for persistence operations. For more information on how to
 * configure a custom serializer, refer to the
 * <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
 * 
 *
 * @see TypeMaker
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
 * @param <V> Type of the attribute associated with the AttributeSerializer
 *
 * @author	Matthias Br&ouml;cheler (http://www.matthiasb.com)
 *
 */
public interface AttributeSerializer<V> {

    /**
     * Reads an attribute from the given ByteBuffer.
     *
     * It is expected that this read operation adjusts the position in the ByteBuffer to after the attribute value.
     *
     * @param buffer ByteBuffer to read attribute from
     * @return Read attribute
     */
	public V read(ByteBuffer buffer);

    /**
     * Writes the attribute value to the given ByteBuffer.
     *
     * It is expected that this write operation adjusts the position in the ByteBuffer to after the attribute value.
     *
     * @param buffer ByteBuffer to write attribute to
     * @param attribute Attribute to write to ByteBuffer
     */
	public void writeObjectData(ByteBuffer buffer, V attribute);
	
}
