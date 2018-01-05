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

public class CharArraySerializer extends ArraySerializer implements AttributeSerializer<char[]> {


    @Override
    public char[] convert(Object value) {
        return convertInternal(value, char.class, Character.class);
    }

    @Override
    protected Object getArray(int length) {
        return new char[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.setChar(array,pos,((Character)value));
    }

    //############### Serialization ###################

    @Override
    public char[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length<0) return null;
        return buffer.getChars(length);
    }

    @Override
    public void write(WriteBuffer buffer, char[] attribute) {
        writeLength(buffer,attribute);
        if (attribute!=null) for (char anAttribute : attribute) buffer.putChar(anAttribute);
    }
}
