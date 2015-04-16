package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ObjectSerializer implements AttributeSerializer<Object> {

    @Override
    public Object read(ScanBuffer buffer) {
        Preconditions.checkArgument(buffer.getByte()==1,"Invalid serialization state");
        return new Object();
    }

    @Override
    public void write(WriteBuffer buffer, Object attribute) {
        buffer.putByte((byte)1);
    }

}
