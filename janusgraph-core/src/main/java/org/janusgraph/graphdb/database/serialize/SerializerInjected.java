package org.janusgraph.graphdb.database.serialize;

/**
 * Marks a {@link org.janusgraph.core.attribute.AttributeSerializer} that requires a {@link com.thinkaurelius.titan.graphdb.database.serialize.Serializer}
 * to serialize the internal state. It is expected that the serializer is passed into this object upon initialization and before usage.
 * Furthermore, such serializers will convert the {@link org.janusgraph.diskstorage.WriteBuffer} passed into the
 * {@link org.janusgraph.core.attribute.AttributeSerializer}'s write methods to be cast to {@link com.thinkaurelius.titan.graphdb.database.serialize.DataOutput}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SerializerInjected {

    public void setSerializer(Serializer serializer);

}
