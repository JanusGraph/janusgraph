package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class KeyValueStoreUtil {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStoreUtil.class);
    public static final Serializer serial = new StandardSerializer();
    public static final long idOffset = 1000;

    public static final StaticBuffer MIN_KEY = BufferUtil.getLongBuffer(0);
    public static final StaticBuffer MAX_KEY = BufferUtil.getLongBuffer(-1);

    public static String[] generateData(int numKeys) {
        String[] ret = new String[numKeys];
        for (int i = 0; i < numKeys; i++) {
            ret[i] = RandomGenerator.randomString();
        }
        return ret;
    }

    public static String[][] generateData(int numKeys, int numColumns) {
        String[][] ret = new String[numKeys][numColumns];
        for (int i = 0; i < numKeys; i++) {
            for (int j = 0; j < numColumns; j++) {
                ret[i][j] = RandomGenerator.randomString();
            }
        }
        return ret;
    }

    public static void print(String[] data) {
        log.debug(Arrays.toString(data));
    }

    public static void print(String[][] data) {
        for (int i = 0; i < data.length; i++) print(data[i]);
    }

    public static StaticBuffer getBuffer(int no) {
        return BufferUtil.getLongBuffer(no + idOffset);
    }

    public static int getID(StaticBuffer b) {
        long res = b.getLong(0) - idOffset;
        Assert.assertTrue(res >= 0 && res < Integer.MAX_VALUE);
        return (int) res;
    }

    public static StaticBuffer getBuffer(String s) {
        DataOutput out = serial.getDataOutput(50);
        out.writeObjectNotNull(s);
        return out.getStaticBuffer();
    }

    public static String getString(ReadBuffer b) {
        return serial.readObjectNotNull(b, String.class);
    }

    public static String getString(StaticBuffer b) {
        return serial.readObjectNotNull(b.asReadBuffer(), String.class);
    }

    public static int count(RecordIterator<?> iterator) throws BackendException {
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

}
