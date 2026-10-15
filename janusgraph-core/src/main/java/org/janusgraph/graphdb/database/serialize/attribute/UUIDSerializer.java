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
import org.janusgraph.graphdb.database.serialize.AttributeUtils;
import org.janusgraph.graphdb.database.serialize.OrderPreservingSerializer;

import java.util.UUID;

/**
 * Writes a UUID as its two halves, the most significant first, each as eight bytes, in byte order too. So the storage
 * backend holds UUIDs which are sort keys by their bits as unsigned numbers, which is the order of their string forms,
 * and in which JanusGraph compares them ({@link AttributeUtils#compareValues}), rather than in that of
 * {@link UUID#compareTo}, which compares each half as a signed number.
 *
 *  @author Bryn Cooke (bryn.cooke@datastax.com)
 */
public class UUIDSerializer implements OrderPreservingSerializer<UUID>  {

    @Override
    public UUID read(ScanBuffer buffer) {
        long mostSignificantBits = buffer.getLong();
        long leastSignificantBits = buffer.getLong();
        return new UUID(mostSignificantBits, leastSignificantBits);
    }

    @Override
    public void write(WriteBuffer buffer, UUID attribute) {
        buffer.putLong(attribute.getMostSignificantBits());
        buffer.putLong(attribute.getLeastSignificantBits());
    }

    @Override
    public UUID convert(Object value) {
        Preconditions.checkNotNull(value);
        if(value instanceof String){
            return UUID.fromString((String) value);
        }
        return null;
    }

    @Override
    public UUID readByteOrder(ScanBuffer buffer) {
        return read(buffer);
    }

    @Override
    public void writeByteOrder(WriteBuffer buffer, UUID attribute) {
        write(buffer, attribute);
    }

}
