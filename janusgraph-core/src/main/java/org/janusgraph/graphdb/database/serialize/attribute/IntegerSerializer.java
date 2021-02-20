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

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.OrderPreservingSerializer;

public class IntegerSerializer implements OrderPreservingSerializer<Integer> {

    private static final long serialVersionUID = 1174998819862504186L;

    @Override
    public Integer read(ScanBuffer buffer) {
        final long l = VariableLong.read(buffer);
        Preconditions.checkArgument(l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE,"Invalid serialization [%s]", l);
        return (int)l;
    }

    @Override
    public void write(WriteBuffer out, Integer attribute) {
        VariableLong.write(out,attribute);
    }

    @Override
    public Integer readByteOrder(ScanBuffer buffer) {
        return buffer.getInt() + Integer.MIN_VALUE;
    }

    @Override
    public void writeByteOrder(WriteBuffer out, Integer attribute) {
        out.putInt(attribute - Integer.MIN_VALUE);
    }

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== byte, short, int, long ======
     */


    @Override
    public Integer convert(Object value) {
        if (value instanceof Number) {
            final double d = ((Number) value).doubleValue();
            Preconditions.checkArgument(!Double.isNaN(d) && Math.round(d) == d, "Not a valid integer: %s", value);
            final long l = ((Number) value).longValue();
            Preconditions.checkArgument(l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE,
                "Value too large for integer: %s", value);
            return (int) l;
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else return null;
    }
}
