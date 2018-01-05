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

public class CharacterSerializer implements OrderPreservingSerializer<Character>  {

    private final ShortSerializer ss = new ShortSerializer();

    @Override
    public Character read(ScanBuffer buffer) {
        final short s = ss.read(buffer);
        return short2char(s);
    }

    @Override
    public void write(WriteBuffer out, Character attribute) {
        ss.write(out, char2short(attribute));
    }

    @Override
    public Character readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Character attribute) {
        write(buffer,attribute);
    }

    public static short char2short(char c) {
        return (short) (((int) c) + Short.MIN_VALUE);
    }

    public static char short2char(short s) {
        return (char) (((int) s) - Short.MIN_VALUE);
    }

    @Override
    public Character convert(Object value) {
        if (value instanceof String && ((String) value).length() == 1) {
            return ((String) value).charAt(0);
        }
        return null;
    }
}
