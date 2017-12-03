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

public class ByteSerializer implements OrderPreservingSerializer<Byte> {

    private static final long serialVersionUID = 117423419883604186L;

    @Override
    public Byte read(ScanBuffer buffer) {
        return (byte) (buffer.getByte() + Byte.MIN_VALUE);
    }

    @Override
    public void write(WriteBuffer out, Byte object) {
        out.putByte((byte)(object - Byte.MIN_VALUE));
    }

    @Override
    public Byte readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Byte attribute) {
        write(buffer,attribute);
    }

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== boolean, byte, short, int, long ======
     */

    @Override
    public Byte convert(Object value) {
        if (value instanceof Number) {
            double d = ((Number)value).doubleValue();
            if (Double.isNaN(d) || Math.round(d)!=d) throw new IllegalArgumentException("Not a valid byte: " + value);
            long l = ((Number)value).longValue();
            if (l>=Byte.MIN_VALUE && l<=Byte.MAX_VALUE) return (byte) l;
            else throw new IllegalArgumentException("Value too large for byte: " + value);
        } else if (value instanceof String) {
            return Byte.parseByte((String)value);
        } else return null;
    }

}
