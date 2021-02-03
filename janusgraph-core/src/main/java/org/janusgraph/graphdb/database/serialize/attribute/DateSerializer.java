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

import org.apache.tinkerpop.shaded.jackson.databind.util.StdDateFormat;

import java.text.ParseException;
import java.util.Date;

public class DateSerializer implements OrderPreservingSerializer<Date> {

    private final LongSerializer ls = LongSerializer.INSTANCE;
    // StdDateFormat is not thread-safe
    private static final ThreadLocal<StdDateFormat> dateFormat = ThreadLocal.withInitial(StdDateFormat::new);

    @Override
    public Date read(ScanBuffer buffer) {
        long utc = ls.read(buffer);
        return new Date(utc);
    }

    @Override
    public void write(WriteBuffer out, Date attribute) {
        long utc = attribute.getTime();
        ls.write(out, utc);
    }

    @Override
    public Date readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, Date attribute) {
        write(buffer,attribute);
    }

    @Override
    public Date convert(Object value) {
        if (value instanceof Number && !(value instanceof Float) && !(value instanceof Double)) {
            return new Date(((Number)value).longValue());
        } else if (value instanceof String) {
            try {
                return dateFormat.get().parse((String) value);
            } catch (ParseException ignored) {
            }
        }
        return null;
    }
}
