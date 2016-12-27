package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.OrderPreservingSerializer;

public class CharacterSerializer implements OrderPreservingSerializer<Character>  {

    private final ShortSerializer ss = new ShortSerializer();

    @Override
    public Character read(ScanBuffer buffer) {
        short s = ss.read(buffer);
        return Character.valueOf(short2char(s));
    }

    @Override
    public void write(WriteBuffer out, Character attribute) {
        ss.write(out, char2short(attribute.charValue()));
    }

    @Override
    public Character readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Character attribute) {
        write(buffer,attribute);
    }

    public static final short char2short(char c) {
        return (short) (((int) c) + Short.MIN_VALUE);
    }

    public static final char short2char(short s) {
        return (char) (((int) s) - Short.MIN_VALUE);
    }

    @Override
    public Character convert(Object value) {
        if (value instanceof String && ((String) value).length() == 1) {
            return Character.valueOf(((String) value).charAt(0));
        }
        return null;
    }
}
