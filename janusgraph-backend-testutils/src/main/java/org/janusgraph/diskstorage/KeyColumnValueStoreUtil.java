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

import org.janusgraph.diskstorage.keycolumnvalue.KCVSUtil;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.WriteByteBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KeyColumnValueStoreUtil {

    public static void delete(KeyColumnValueStore store, StoreTransaction txn, long key, String col) throws BackendException {
        StaticBuffer k = longToByteBuffer(key);
        StaticBuffer c = stringToByteBuffer(col);
        store.mutate(k, KeyColumnValueStore.NO_ADDITIONS, Collections.singletonList(c), txn);
    }

    public static String get(KeyColumnValueStore store, StoreTransaction txn, long key, String col) throws BackendException {
        StaticBuffer k = longToByteBuffer(key);
        StaticBuffer c = stringToByteBuffer(col);
        StaticBuffer valBytes = KCVSUtil.get(store, k, c, txn);
        if (null == valBytes)
            return null;
        return byteBufferToString(valBytes);
    }

    public static void insert(KeyColumnValueStore store, StoreTransaction txn, long key, String col, String val) throws BackendException {
        StaticBuffer k = longToByteBuffer(key);
        StaticBuffer c = stringToByteBuffer(col);
        StaticBuffer v = stringToByteBuffer(val);
        store.mutate(k, Collections.singletonList(StaticArrayEntry.of(c, v)), KeyColumnValueStore.NO_DELETIONS, txn);
    }

    public static void loadValues(KeyColumnValueStore store, StoreTransaction tx, String[][] values) throws BackendException {
        loadValues(store, tx, values, -1, -1);
    }

    public static void loadValues(KeyColumnValueStore store, StoreTransaction tx, String[][] values, int shiftEveryNthRow,
                           int shiftSliceLength) throws BackendException {
        for (int i = 0; i < values.length; i++) {

            final List<Entry> entries = new ArrayList<>();
            for (int j = 0; j < values[i].length; j++) {
                StaticBuffer col;
                if (0 < shiftEveryNthRow && 0 == i/* +1 */ % shiftEveryNthRow) {
                    ByteBuffer bb = ByteBuffer.allocate(shiftSliceLength + 9);
                    for (int s = 0; s < shiftSliceLength; s++) {
                        bb.put((byte) -1);
                    }
                    bb.put(KeyValueStoreUtil.getBuffer(j + 1).asByteBuffer());
                    bb.flip();
                    col = StaticArrayBuffer.of(bb);

                    // col = KeyValueStoreUtil.getBuffer(j + values[i].length +
                    // 100);
                } else {
                    col = KeyValueStoreUtil.getBuffer(j);
                }
                entries.add(StaticArrayEntry.of(col, KeyValueStoreUtil
                        .getBuffer(values[i][j])));
            }
            if (!entries.isEmpty()) {
                store.mutate(KeyValueStoreUtil.getBuffer(i), entries,
                        KeyColumnValueStore.NO_DELETIONS, tx);
            }
        }
    }

    // TODO rename as "bufferToString" after syntax errors are resolved
    public static String byteBufferToString(StaticBuffer b) {
        ByteBuffer bb = b.asByteBuffer();
        return new String(bb.array(), bb.position() + bb.arrayOffset(), bb.remaining(), StandardCharsets.UTF_8);
    }

    // TODO rename as "stringToBuffer" after syntax errors are resolved
    public static StaticBuffer stringToByteBuffer(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(b.length);
        bb.put(b);
        bb.flip();
        return StaticArrayBuffer.of(bb);
    }

    // TODO rename as "longToBuffer" after syntax errors are resolved
    public static StaticBuffer longToByteBuffer(long l) {
        return new WriteByteBuffer(8).putLong(l).getStaticBuffer();
    }
    
    public static long bufferToLong(StaticBuffer b) {
        return b.getLong(0);
    }
}
