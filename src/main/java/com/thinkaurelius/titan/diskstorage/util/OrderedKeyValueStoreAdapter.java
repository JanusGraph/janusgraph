package com.thinkaurelius.titan.diskstorage.util;

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
		return convert(store.getSlice(concatenate(key,columnStart), concatenate(key,columnEnd), limit, txh));
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, 
			TransactionHandle txh) {
		return convert(store.getSlice(concatenate(key,columnStart), concatenate(key,columnEnd), txh));
	}
		
	
	public List<Entry> convert(List<KeyValueEntry> entries) {
		if (entries==null) return null;
		List<Entry> newentries = new ArrayList<Entry>(entries.size());
		for (KeyValueEntry entry : entries) {
			newentries.add(new Entry(getColumn(entry.getKey()),entry.getValue()));
		}
		return newentries;
	}
	
	public Map<ByteBuffer,List<Entry>> convertKey(List<KeyValueEntry> entries) {
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

		private final ByteBuffer keyStart;
		private final ByteBuffer keyEnd;
		private final boolean startKeyInc;
		private final boolean endKeyInc;
		private final ByteBuffer columnStart;
		private final ByteBuffer columnEnd;
		private final boolean startColumnIncl;
		private final boolean endColumnIncl;
		private final int keyLimit;
		private final int columnLimit;
		
		private final boolean abortOnLimitExcess;
		
		public KeyColumnSliceSelector(ByteBuffer keyStart, ByteBuffer keyEnd, 
				boolean startKeyInc, boolean endKeyInc,
			ByteBuffer columnStart, ByteBuffer columnEnd, 
			boolean startColumnIncl, boolean endColumnIncl, 
			int keyLimit, int columnLimit, boolean abortOnLimit) {
			this.keyStart=keyStart;
			this.keyEnd=keyEnd;
			this.startKeyInc=startKeyInc;
			this.endKeyInc=endKeyInc;
			this.columnStart=columnStart;
			this.columnEnd=columnEnd;
			this.startColumnIncl=startColumnIncl;
			this.endColumnIncl=endColumnIncl;
			this.keyLimit=keyLimit;
			this.columnLimit=columnLimit;
			this.abortOnLimitExcess=abortOnLimit;
		}
		
		private ByteBuffer currentKey = null;
		
		private int countKey = 0;
		private int countColumn = 0;

		private boolean reachedLimit = false;
		private boolean reachedCountLimit = false;
		
		@Override
		public boolean include(ByteBuffer keycolumn) {
			if (currentKey==null && !startKeyInc) { //Check if current equals start
				if (equalKey(keycolumn, keyStart)) return false;
			}
			
			if (!endKeyInc && equalKey(keycolumn, keyEnd)) {
				reachedLimit = true;
				return false;
			}
			
			if (currentKey==null || !equalKey(keycolumn,currentKey)) {
				currentKey = getKey(keycolumn);
				countKey++;
				if (countKey>keyLimit) {
					reachedCountLimit = true;
					reachedLimit = true;
					return false;
				}
				countColumn = 0;
			}
			
			if (countColumn>=columnLimit) return false;
			
			if (columnInRange(keycolumn,columnStart,columnEnd,
					startColumnIncl,endColumnIncl)) {
				countColumn++;
				if (countColumn>=columnLimit) {
					reachedCountLimit=true;
					if (abortOnLimitExcess) reachedLimit=true;
				}
				return true;
			} else return false;
		}

		@Override
		public boolean reachedLimit() {
			return reachedLimit;
		}
		
		
		public boolean reachedCountLimit() {
			return reachedCountLimit;
		}
		
	}


}
