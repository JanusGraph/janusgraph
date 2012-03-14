package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Util {
	public static void delete(KeyColumnValueStore store, TransactionHandle txn, long key, String col) {
		ByteBuffer k = longToByteBuffer(key);
		ByteBuffer c = stringToByteBuffer(col);
		store.delete(k, Arrays.asList(c), txn);
	}
	
	public static String get(KeyColumnValueStore store, TransactionHandle txn, long key, String col) {
		ByteBuffer k = longToByteBuffer(key);
		ByteBuffer c = stringToByteBuffer(col);
		ByteBuffer valBytes = store.get(k, c, txn);
		if (null == valBytes)
			return null;
		return byteBufferToString(valBytes);
	}
	
	public static void insert(KeyColumnValueStore store, TransactionHandle txn, long key, String col, String val) {
		ByteBuffer k = longToByteBuffer(key);
		ByteBuffer c = stringToByteBuffer(col);
		ByteBuffer v = stringToByteBuffer(val);
		store.insert(k, Arrays.asList(new Entry(c, v)), txn);
	}
	
    public static String byteBufferToString(ByteBuffer b) {
    	try {
			return new String(b.array(), b.position() + b.arrayOffset(), b.remaining(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
    }

    public static ByteBuffer stringToByteBuffer(String s) {
    	byte[] b;
		try {
			b = s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
    	ByteBuffer bb = ByteBuffer.allocate(b.length);
    	bb.put(b);
    	bb.flip();
    	return bb;
    }
    
    public static ByteBuffer longToByteBuffer(long l) {
    	ByteBuffer b = ByteBuffer.allocate(8).putLong(l);
    	b.flip();
    	return b;
    }
}
