package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;

import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.SerializerInjected;
import com.thinkaurelius.titan.graphdb.log.StandardTransactionId;

import java.time.Instant;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardTransactionIdSerializer implements AttributeSerializer<StandardTransactionId>, SerializerInjected {

    private Serializer serializer;

    @Override
    public StandardTransactionId read(ScanBuffer buffer) {
        return new StandardTransactionId(serializer.readObjectNotNull(buffer,String.class),
                serializer.readObjectNotNull(buffer,Long.class),
                serializer.readObjectNotNull(buffer,Instant.class));
    }

    @Override
    public void write(WriteBuffer buffer, StandardTransactionId attribute) {
        DataOutput out = (DataOutput)buffer;
        out.writeObjectNotNull(attribute.getInstanceId());
        out.writeObjectNotNull(attribute.getTransactionId());
        out.writeObjectNotNull(attribute.getTransactionTime());
    }

    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer=serializer;
    }
}
