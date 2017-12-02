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

import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;

import java.lang.reflect.Array;

public class DoubleArraySerializer extends ArraySerializer implements AttributeSerializer<double[]> {

    @Override
    public double[] convert(Object value) {
        return convertInternal(value, double.class, Double.class);
    }

    @Override
    protected Object getArray(int length) {
        return new double[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.setDouble(array,pos,((Double)value));
    }

    //############### Serialization ###################

    @Override
    public double[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length<0) return null;
        return buffer.getDoubles(length);
    }

    @Override
    public void write(WriteBuffer buffer, double[] attribute) {
        writeLength(buffer,attribute);
        if (attribute!=null) for (double anAttribute : attribute) buffer.putDouble(anAttribute);
    }
}
