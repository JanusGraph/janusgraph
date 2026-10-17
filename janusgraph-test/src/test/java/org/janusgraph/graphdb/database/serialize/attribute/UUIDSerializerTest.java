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

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.ReadArrayBuffer;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.graphdb.database.serialize.AttributeUtils;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Bryn Cooke (bryn.cooke@datastax.com)
 */
public class UUIDSerializerTest {

    @Test
    public void testRoundTrip() {
        //Write the UUID
        UUIDSerializer serializer = new UUIDSerializer();
        UUID uuid1 = UUID.randomUUID();
        WriteByteBuffer buffer = new WriteByteBuffer();
        serializer.write(buffer, uuid1);

        //And read it in again
        ReadArrayBuffer readBuffer = new ReadArrayBuffer(buffer.getStaticBuffer().getBytes(0, 16));
        UUID uuid2 = serializer.read(readBuffer);

        assertEquals(uuid1, uuid2);
    }

    @Test
    public void testRoundTripByteOrder() {
        //Write the UUID
        UUIDSerializer serializer = new UUIDSerializer();
        UUID uuid1 = UUID.randomUUID();
        WriteByteBuffer buffer = new WriteByteBuffer();
        serializer.writeByteOrder(buffer, uuid1);

        //And read it in again
        ReadArrayBuffer readBuffer = new ReadArrayBuffer(buffer.getStaticBuffer().getBytes(0, 16));
        UUID uuid2 = serializer.readByteOrder(readBuffer);

        assertEquals(uuid1, uuid2);
    }

    /**
     * The byte order of UUIDs is that of their bits as unsigned numbers, the order of their string forms, in which
     * JanusGraph compares them, rather than that of UUID.compareTo, which compares them as signed numbers.
     */
    @Test
    public void testByteOrderIsTheOrderOfTheStringForms() {
        final UUIDSerializer serializer = new UUIDSerializer();
        final Random random = new Random(3);
        for (int n = 0; n < 20000; n++) {
            final UUID u1 = new UUID(random.nextLong(), random.nextLong());
            final UUID u2 = random.nextBoolean() ? new UUID(u1.getMostSignificantBits(), random.nextLong())
                : new UUID(random.nextLong(), random.nextLong());
            final int byString = Integer.signum(u1.toString().compareTo(u2.toString()));
            assertEquals(byString, Integer.signum(byteOrder(serializer, u1).compareTo(byteOrder(serializer, u2))),
                u1 + " vs " + u2);
            assertEquals(byString, Integer.signum(AttributeUtils.compareValues(u1, u2)), u1 + " vs " + u2);
        }
    }

    private static StaticBuffer byteOrder(UUIDSerializer serializer, UUID uuid) {
        final WriteByteBuffer buffer = new WriteByteBuffer();
        serializer.writeByteOrder(buffer, uuid);
        return buffer.getStaticBuffer();
    }

    @Test
    public void testConvert() {
        //Write the UUID
        UUIDSerializer serializer = new UUIDSerializer();
        UUID parsed = serializer.convert("d320e751-3a9c-48a8-88f5-2c8b455baa5f");
        assertEquals(UUID.fromString("d320e751-3a9c-48a8-88f5-2c8b455baa5f"), parsed);
    }
}
