package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BatchKeyColumnValueStoreMutator
	implements KeyColumnValueStoreMutator {

	private TransactionHandle txh;

	private int curInserts;
	private int curDeletes;
	private final int maxBatch;
	private final MultiWriteKeyColumnValueStore store;
	private final Map<ByteBuffer, List<Entry>> inserts =
		new HashMap<ByteBuffer, List<Entry>>();
	private final Map<ByteBuffer, List<ByteBuffer>> deletes =
		new HashMap<ByteBuffer, List<ByteBuffer>>();
	
	public BatchKeyColumnValueStoreMutator(TransactionHandle txh, MultiWriteKeyColumnValueStore store, int maxBatch) {
		this.txh = txh;
		this.store = store;
		this.maxBatch = maxBatch;
	}

	@Override
	public void insert(ByteBuffer key, List<Entry> entries) {
		/*
		 * The assertion below fails in StandardGraphDBTest.
		 * 
		 * Therefore, we need to support multiple batchInsert()
		 * calls on the same key without dropping entries.
		 */
//		List<Entry> old = inserts.put(key, entries);
//		assert null == old;
		List<Entry> batch = inserts.get(key);
		if (null == batch) {
			batch = new LinkedList<Entry>();
			inserts.put(key, batch);
		}
		batch.addAll(entries);
		curInserts += entries.size();
		if (maxBatch <= curInserts)
			flushInserts();
	}

	@Override
	public void delete(ByteBuffer key, List<ByteBuffer> columns) {
		/*
		 * The assertion below fails in StandardGraphDBTest.
		 * 
		 * Therefore, we need to support multiple batchInsert()
		 * calls on the same key without dropping entries.
		 */
//		List<ByteBuffer> old = deletes.put(key, columns);
//		assert null == old;
		List<ByteBuffer> batch = deletes.get(key);
		if (null == batch) {
			batch = new LinkedList<ByteBuffer>();
			deletes.put(key, batch);
		}
		batch.addAll(columns);
		curDeletes += columns.size();
		if (maxBatch <= curDeletes)
			flushDeletes();
	}

	@Override
	public void flushInserts() {
		store.insertMany(inserts, txh);
		inserts.clear();
		curInserts = 0;
	}

	@Override
	public void flushDeletes() {
		store.deleteMany(deletes, txh);
		deletes.clear();
		curDeletes = 0;
	}

	@Override
	public void flush() {
		flushDeletes();
		flushInserts();
	}
}
