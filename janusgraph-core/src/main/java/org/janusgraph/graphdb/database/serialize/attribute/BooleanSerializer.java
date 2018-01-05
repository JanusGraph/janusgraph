// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.database.serialize.attribute;

import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.OrderPreservingSerializer;

public class BooleanSerializer implements OrderPreservingSerializer<Boolean> {

    @Override
    public Boolean read(ScanBuffer buffer) {
        return decode(buffer.getByte());
    }

    @Override
    public void write(WriteBuffer out, Boolean attribute) {
        out.putByte(encode(attribute));
    }


    @Override
    public Boolean readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Boolean attribute) {
        write(buffer,attribute);
    }

    @Override
    public Boolean convert(Object value) {
        if (value instanceof Number) {
            return decode(((Number)value).byteValue());
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String)value);
        } else return null;
    }

    public static boolean decode(byte b) {
        switch (b) {
            case 0: return false;
            case 1: return true;
            default: throw new IllegalArgumentException("Invalid boolean value: " + b);
        }
    }

    public static byte encode(boolean b) {
        return (byte)(b?1:0);
    }
}
