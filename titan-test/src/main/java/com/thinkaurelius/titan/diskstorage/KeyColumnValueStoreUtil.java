package com.thinkaurelius.titan.diskstorage;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVSUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

public class KeyColumnValueStoreUtil {

    public static void delete(KeyColumnValueStore store, StoreTransaction txn, long key, String col) throws StorageException {
        StaticBuffer k = longToByteBuffer(key);
        StaticBuffer c = stringToByteBuffer(col);
        store.mutate(k, KeyColumnValueStore.NO_ADDITIONS, Arrays.asList(c), txn);
    }

    public static String get(KeyColumnValueStore store, StoreTransaction txn, long key, String col) throws StorageException {
        StaticBuffer k = longToByteBuffer(key);
        StaticBuffer c = stringToByteBuffer(col);
        StaticBuffer valBytes = KCVSUtil.get(store, k, c, txn);
        if (null == valBytes)
            return null;
        return byteBufferToString(valBytes);
    }

    public static void insert(KeyColumnValueStore store, StoreTransaction txn, long key, String col, String val) throws StorageException {
        StaticBuffer k = longToByteBuffer(key);
        StaticBuffer c = stringToByteBuffer(col);
        StaticBuffer v = stringToByteBuffer(val);
        store.mutate(k, Arrays.<Entry>asList(new StaticBufferEntry(c, v)), KeyColumnValueStore.NO_DELETIONS, txn);
    }

    // TODO rename as "bufferToString" after syntax errors are resolved
    public static String byteBufferToString(StaticBuffer b) {
        try {
            ByteBuffer bb = b.asByteBuffer();
            return new String(bb.array(), bb.position() + bb.arrayOffset(), bb.remaining(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO rename as "stringToBuffer" after syntax errors are resolved
    public static StaticBuffer stringToByteBuffer(String s) {
        byte[] b;
        try {
            b = s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        ByteBuffer bb = ByteBuffer.allocate(b.length);
        bb.put(b);
        bb.flip();
        return new StaticByteBuffer(bb);
    }

    // TODO rename as "longToBuffer" after syntax errors are resolved
    public static StaticBuffer longToByteBuffer(long l) {
        ByteBuffer b = ByteBuffer.allocate(8).putLong(l);
        b.flip();
        return new StaticByteBuffer(b);
    }
    
    public static long bufferToLong(StaticBuffer b) {
        return b.getLong(0);
    }
}
