package com.thinkaurelius.titan.diskstorage.cassandra;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.testcategory.StandaloneTests;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@Category({StandaloneTests.class})
public class UUIDTest {
    public static final String z = "00000000-0000-1000-0000-000000000000";
    public static final String v = "9451e273-7753-11e0-92df-e700f669bcfc";

    @Test
    public void timeUUIDComparison() {
        TimeUUIDType ti = TimeUUIDType.instance;

        UUID zu = UUID.fromString(z);
        UUID vu = UUID.fromString(v);

        ByteBuffer zb = ti.decompose(zu);
        ByteBuffer vb = ti.decompose(vu);

        assertEquals(-1, ti.compare(zb, vb));
        assertEquals(1, zu.compareTo(vu));
        assertEquals(1, ti.compare(vb, zb));
        assertEquals(-1, vu.compareTo(zu));
    }
}
