package org.janusgraph.graphdb.serializer.attributes;

import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.attribute.StringSerializer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TClass2Serializer implements AttributeSerializer<TClass2> {

    private final StringSerializer strings = new StringSerializer();

    @Override
    public TClass2 read(ScanBuffer buffer) {
        return new TClass2(strings.read(buffer),buffer.getInt());
    }

    @Override
    public void write(WriteBuffer buffer, TClass2 attribute) {
        strings.write(buffer,attribute.getS());
        buffer.putInt(attribute.getI());
    }

}
