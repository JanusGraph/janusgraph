package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class CharacterSerializer implements AttributeSerializer<Character> {

    private final ShortSerializer ss = new ShortSerializer();

    @Override
    public Character read(ScanBuffer buffer) {
        short s = ss.read(buffer);
        return Character.valueOf((char)(s-Short.MIN_VALUE));
    }

    @Override
    public void writeObjectData(WriteBuffer out, Character attribute) {
        ss.writeObjectData(out,(short)(attribute.charValue()+Short.MIN_VALUE));
    }

}
