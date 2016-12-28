package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.diskstorage.util.ReadArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

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

        Assert.assertEquals(uuid1, uuid2);
    }

    @Test
    public void testConvert() {
        //Write the UUID
        UUIDSerializer serializer = new UUIDSerializer();
        UUID parsed = serializer.convert("d320e751-3a9c-48a8-88f5-2c8b455baa5f");
        Assert.assertEquals(UUID.fromString("d320e751-3a9c-48a8-88f5-2c8b455baa5f"), parsed);
    }

}
