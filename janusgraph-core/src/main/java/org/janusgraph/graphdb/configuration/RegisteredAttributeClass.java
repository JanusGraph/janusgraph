package org.janusgraph.graphdb.configuration;

import com.google.common.base.Preconditions;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.graphdb.database.serialize.Serializer;

/**
 * Helper class for registering data types with JanusGraph
 *
 * @param <T>
 */
public class RegisteredAttributeClass<T> {

    private final int position;
    private final Class<T> type;
    private final AttributeSerializer<T> serializer;

    public RegisteredAttributeClass(int position, Class<T> type, AttributeSerializer<T> serializer) {
        Preconditions.checkArgument(position>=0,"Position must be a positive integer, given: %s",position);
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(serializer);
        this.position=position;
        this.type = type;
        this.serializer = serializer;
    }


    void registerWith(Serializer s) {
        s.registerClass(position,type,serializer);
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) return true;
        else if (oth==null || !getClass().isInstance(oth)) return false;
        return type.equals(((RegisteredAttributeClass<?>) oth).type);
    }
    
    @Override
    public int hashCode() {
        return type.hashCode() + 110432;
    }

    @Override
    public String toString() {
        return type.toString();
    }
}
