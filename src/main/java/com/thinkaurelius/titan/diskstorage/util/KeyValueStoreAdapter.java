package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KeyValueStoreAdapter implements KeyColumnValueStore {

	public static final int variableKeyLength = 0;
	
	public static final int maxVariableKeyLength = Short.MAX_VALUE;
	public static final int variableKeyLengthSize = 2;
	
	protected final KeyValueStore store;
	protected final int keyLength;
	
	public KeyValueStoreAdapter(KeyValueStore store) {
		this(store,variableKeyLength);
	}
	
	public KeyValueStoreAdapter(KeyValueStore store, int keyLength) {
		Preconditions.checkNotNull(store);
		Preconditions.checkArgument(keyLength>=0);
		this.store=store;
		this.keyLength = keyLength;
	}
	
	public final int getKeyLength() {
		return keyLength;
	}
	
	protected final ByteBuffer concatenate(ByteBuffer front, ByteBuffer end) {
		int length = keyLength;
		if (keyLength>0) { //fixed key length
			Preconditions.checkArgument(front.remaining()==length);
		} else { //variable key length
			length = front.remaining();
			Preconditions.checkArgument(length<maxVariableKeyLength);
		}
		
		ByteBuffer result = ByteBuffer.allocate(length + end.remaining() + ((keyLength>0)?0:variableKeyLengthSize));
		
		front.mark();
		result.put(front);
		front.reset();
		end.mark();
		result.put(end);
		end.reset();
		
		if (keyLength<=0) result.putShort((short)length);
		
		result.flip();
		front.reset(); end.reset();
		return result;
	}
	
	protected final ByteBuffer getColumn(ByteBuffer concat) {
		concat.position(getKeyLength(concat));
		ByteBuffer column = concat.slice();
		if (keyLength<=0) { //variable key length => remove length at end
			column.limit(column.limit()-variableKeyLengthSize);
		}
		return column;
	}

	protected final int getKeyLength(ByteBuffer concat) {
		int length = keyLength;
		if (keyLength<=0) { //variable key length
			length = concat.getShort(concat.limit()-variableKeyLengthSize);
		}
		return length;
	}
	
	protected final ByteBuffer getKey(ByteBuffer concat) {
		ByteBuffer key = concat.duplicate();
		key.limit(key.position()+getKeyLength(concat));
		return key;
	}
	
	protected final boolean equalKey(ByteBuffer concat, ByteBuffer key) {
		int oldlimit = concat.limit();
		concat.limit(concat.position()+getKeyLength(concat));
		boolean equals = key.equals(concat);
		concat.limit(oldlimit);
		return equals;
	}
	
	protected final boolean columnInRange(ByteBuffer concat, ByteBuffer columnStart,
			ByteBuffer columnEnd, boolean startInc, boolean endInc) {
		int oldposition = concat.position(), oldlimit = concat.limit();
		concat.position(getKeyLength(concat));
		if (keyLength<=0) concat.limit(concat.limit()-variableKeyLengthSize);
		boolean inrange = 
				ByteBufferUtil.isSmallerThanWithEqual(columnStart, concat, startInc) &&
				ByteBufferUtil.isSmallerThanWithEqual(concat, columnEnd, endInc);
		
		concat.position(oldposition);
		concat.limit(oldlimit);
		return inrange;
	}


    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, TransactionHandle txh) {
        if (deletions!=null) delete(key,deletions,txh);
        if (additions!=null) insert(key,additions,txh);
    }
    
    
	public void delete(ByteBuffer key, List<ByteBuffer> columns,
			TransactionHandle txh) {
		List<ByteBuffer> deleted = new ArrayList<ByteBuffer>(columns.size());
		for (ByteBuffer column : columns) {
			ByteBuffer del = concatenate(key,column);
			deleted.add(del);
		}
		store.delete(deleted, txh);
	}
	
	public void insert(ByteBuffer key, List<Entry> entries,
			TransactionHandle txh) {
		List<KeyValueEntry> newentries = new ArrayList<KeyValueEntry>(entries.size());
		for (Entry entry : entries) {
			ByteBuffer newkey = concatenate(key,entry.getColumn());
			newentries.add(new KeyValueEntry(newkey,entry.getValue()));
		}
		store.insert(newentries, txh);
	}
	
	@Override
	public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		return store.containsKey(concatenate(key,column), txh);
	}

	@Override
	public ByteBuffer get(ByteBuffer key, ByteBuffer column,
			TransactionHandle txh) {
		return store.get(concatenate(key,column), txh);
	}


	@Override
	public boolean isLocalKey(ByteBuffer key) {
		return store.isLocalKey(key);
	}
	
	@Override
	public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue,
			TransactionHandle txh) {
		store.acquireLock(concatenate(key,column), expectedValue, txh);
	}


	@Override
	public void close() throws GraphStorageException {
		store.close();
	}
}
