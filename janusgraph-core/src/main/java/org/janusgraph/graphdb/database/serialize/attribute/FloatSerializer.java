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
import org.janusgraph.util.encoding.NumericUtils;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class FloatSerializer implements OrderPreservingSerializer<Float> {

    private final IntegerSerializer ints = new IntegerSerializer();

    @Override
    public Float convert(Object value) {
        if (value instanceof Number) {
            final double d = ((Number)value).doubleValue();
            if (d < -Float.MAX_VALUE || d > Float.MAX_VALUE) throw new IllegalArgumentException("Value too large for float: " + value);
            return (float) d;
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        } else return null;
    }

    @Override
    public Float read(ScanBuffer buffer) {
        return buffer.getFloat();
    }

    @Override
    public void write(WriteBuffer buffer, Float attribute) {
        buffer.putFloat(attribute);
    }

    @Override
    public Float readByteOrder(ScanBuffer buffer) {
        return NumericUtils.sortableIntToFloat(ints.readByteOrder(buffer));
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Float attribute) {
        ints.writeByteOrder(buffer, NumericUtils.floatToSortableInt(attribute));
    }
}
