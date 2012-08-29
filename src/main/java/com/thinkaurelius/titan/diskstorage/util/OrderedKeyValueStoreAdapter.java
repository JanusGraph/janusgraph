package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrderedKeyValueStoreAdapter extends KeyValueStoreAdapter implements OrderedKeyColumnValueStore{

	protected final OrderedKeyValueStore store;
	
	public OrderedKeyValueStoreAdapter(OrderedKeyValueStore store) {
		super(store);
		this.store=store;
	}
	
	public OrderedKeyValueStoreAdapter(OrderedKeyValueStore store, int keyLength) {
		super(store,keyLength);
		this.store=store;
	}
	
	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {
		ContainsSelector select = new ContainsSelector(key);
		store.getSlice(key, ByteBufferUtil.nextBiggerBuffer(key), select, txh);
		return select.contains();
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, 
			int limit, TransactionHandle txh) {
		return convert(store.getSlice(concatenatePrefix(key,columnStart), concatenatePrefix(key,columnEnd), new KeyColumnSliceSelector(key,limit), txh));
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, 
			TransactionHandle txh) {
		return convert(store.getSlice(concatenatePrefix(key,columnStart), concatenatePrefix(key,columnEnd), new KeyColumnSliceSelector(key), txh));
	}
		
	
	private List<Entry> convert(List<KeyValueEntry> entries) {
		if (entries==null) return null;
		List<Entry> newentries = new ArrayList<Entry>(entries.size());
		for (KeyValueEntry entry : entries) {
			newentries.add(new Entry(getColumn(entry.getKey()),entry.getValue()));
		}
		return newentries;
	}
	
	private Map<ByteBuffer,List<Entry>> convertKey(List<KeyValueEntry> entries) {
		if (entries==null) return null;
		Map<ByteBuffer,List<Entry>> keyentries = new HashMap<ByteBuffer,List<Entry>>((int)Math.sqrt(entries.size()));
		ByteBuffer key = null;
		List<Entry> newentries = null;
		for (KeyValueEntry entry : entries) {
			ByteBuffer currentKey = getKey(entry.getKey());
			if (key==null || !key.equals(currentKey)) {
				if (key!=null) {
					assert newentries!=null;
					keyentries.put(key, newentries);
				}
				key = currentKey;
				newentries = new ArrayList<Entry>((int)Math.sqrt(entries.size()));
			}
			newentries.add(new Entry(getColumn(entry.getKey()),entry.getValue()));
		}
		if (key!=null) {
			assert newentries!=null;
			keyentries.put(key, newentries);
		}
		return keyentries;
	}
	
//	protected final ByteBuffer getMaxKey(ByteBuffer key) {
//		int len = key.remaining();
//		ByteBuffer max = ByteBuffer.allocate(len);
//		for (int i=0;i<len;i++) {
//			max.put(Byte.MIN_VALUE);
//		}
//		max.flip();
//		return max;
//	}

	private class ContainsSelector implements KeySelector {

		private final ByteBuffer checkKey;
		private boolean contains = false;
		
		private ContainsSelector(ByteBuffer key) {
			checkKey = key;
		}
		
		public boolean contains() {
			return contains;
		}
		
		@Override
		public boolean include(ByteBuffer keycolumn) {
			contains = equalKey(keycolumn, checkKey);
			return false;
		}

		@Override
		public boolean reachedLimit() {
			return true;
		}
		
	}
	
	private class KeyColumnSliceSelector implements KeySelector {

		private final ByteBuffer key;
		private final int limit;
		
		public KeyColumnSliceSelector(ByteBuffer key, int limit) {
            Preconditions.checkArgument(limit>0,"The count limit needs to be positive. Given: " + limit);
			this.key = key;
            this.limit=limit;
		}
        
        public KeyColumnSliceSelector(ByteBuffer key) {
            this(key,Integer.MAX_VALUE);
        }
		
		private int count = 0;

		@Override
		public boolean include(ByteBuffer keycolumn) {
            Preconditions.checkArgument(count<limit);
            if (equalKey(keycolumn, key)) {
                count++;
                return true;
            } else return false;
		}

		@Override
		public boolean reachedLimit() {
			return count>=limit;
		}

	}


}
