package com.thinkaurelius.titan.graphdb.serializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.thinkaurelius.titan.graphdb.serializer.attributes.*;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;

public class SerializerTestCommon {

    private static final Logger log =
            LoggerFactory.getLogger(SerializerTestCommon.class);

    protected Serializer serialize;
    protected boolean printStats;

    @Before
    public void setUp() throws Exception {
        serialize = new StandardSerializer();
        printStats = false;
    }

    @After
    public void tearDown() throws Exception {
        serialize.close();
    }

    protected void objectWriteRead() {
        TClass1 t1 = new TClass1(3245234223423433123l,0.333f);
        TClass2 t2 = new TClass2("This is a test",4234234);
        TEnum t3 = TEnum.THREE;
        TEnum t4 = TEnum.TWO;

        DataOutput out = serialize.getDataOutput(128);
        out.writeObjectNotNull(t1);
        out.writeClassAndObject(t2);
        out.writeObject(t3,TEnum.class);
        out.writeClassAndObject(t4);

        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        assertEquals(t1, serialize.readObjectNotNull(b, TClass1.class));
        assertEquals(t2, (TClass2)serialize.readClassAndObject(b));
        assertEquals(t3, serialize.readObject(b,TEnum.class));
        assertEquals(t4, serialize.readClassAndObject(b));

        assertFalse(b.hasRemaining());
    }

    protected void multipleStringWrite() {
        String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; //26 chars
        int no = 100;
        DataOutput out = serialize.getDataOutput(128);
        for (int i = 0; i < no; i++) {
            String str = base + (i + 1);
            out.writeObjectNotNull(str);
        }
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        if (printStats) log.debug(bufferStats(b));
        for (int i = 0; i < no; i++) {
            String str = base + (i + 1);
            String read = serialize.readObjectNotNull(b, String.class);
            assertEquals(str, read);
        }
        assertFalse(b.hasRemaining());
    }


    protected String bufferStats(ReadBuffer b) {
        return "ReadBuffer length: " + b.length();
    }

}
