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

public class DoubleSerializer implements OrderPreservingSerializer<Double> {

    private final LongSerializer longs = new LongSerializer();

    @Override
    public Double convert(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        } else return null;
    }

    @Override
    public Double read(ScanBuffer buffer) {
        return buffer.getDouble();
    }

    @Override
    public void write(WriteBuffer buffer, Double attribute) {
        buffer.putDouble(attribute);
    }

    @Override
    public Double readByteOrder(ScanBuffer buffer) {
        return NumericUtils.sortableLongToDouble(longs.readByteOrder(buffer));
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Double attribute) {
        longs.writeByteOrder(buffer, NumericUtils.doubleToSortableLong(attribute));
    }
}
