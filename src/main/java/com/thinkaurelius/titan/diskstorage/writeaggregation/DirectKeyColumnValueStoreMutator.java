package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.List;

public class DirectKeyColumnValueStoreMutator implements KeyColumnValueStoreMutator {

	private TransactionHandle txh;
	private final KeyColumnValueStore store;
	
	public DirectKeyColumnValueStoreMutator(TransactionHandle txh, KeyColumnValueStore store) {
		this.txh = txh;
		this.store = store;
	}

    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) {
        store.mutate(key,additions,deletions,txh);
    }

	@Override
	public void flush() {
		// Do nothing
	}
}
