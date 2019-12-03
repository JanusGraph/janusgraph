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

package org.janusgraph.diskstorage;

import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;
import org.janusgraph.testutil.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
        for (String[] aData : data) print(aData);
    }

    public static StaticBuffer getBuffer(int no) {
        return BufferUtil.getLongBuffer(no + idOffset);
    }

    public static int getID(StaticBuffer b) {
        long res = b.getLong(0) - idOffset;
        assertTrue(res >= 0 && res < Integer.MAX_VALUE);
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

    public static int count(RecordIterator<?> iterator) {
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

}
