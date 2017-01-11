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
        long l = VariableLong.read(buffer);
        Preconditions.checkArgument(l>=Integer.MIN_VALUE && l<=Integer.MAX_VALUE,"Invalid serialization [%s]",l);
        return (int)l;
    }

    @Override
    public void write(WriteBuffer out, Integer attribute) {
        VariableLong.write(out,attribute);
    }

    @Override
    public Integer readByteOrder(ScanBuffer buffer) {
        return Integer.valueOf(buffer.getInt() + Integer.MIN_VALUE);
    }

    @Override
    public void writeByteOrder(WriteBuffer out, Integer attribute) {
        out.putInt(attribute.intValue() - Integer.MIN_VALUE);
    }

    /*
    ====== These methods apply to all whole numbers with minor modifications ========
    ====== byte, short, int, long ======
     */


    @Override
    public Integer convert(Object value) {
        if (value instanceof Number) {
            double d = ((Number)value).doubleValue();
            if (Double.isNaN(d) || Math.round(d)!=d) throw new IllegalArgumentException("Not a valid integer: " + value);
            long l = ((Number)value).longValue();
            if (l>=Integer.MIN_VALUE && l<=Integer.MAX_VALUE) return Integer.valueOf((int)l);
            else throw new IllegalArgumentException("Value too large for integer: " + value);
        } else if (value instanceof String) {
            return Integer.parseInt((String)value);
        } else return null;
    }
}
