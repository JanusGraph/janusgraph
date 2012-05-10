package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.List;

public class DirectStoreMutator implements StoreMutator {

	private TransactionHandle txh;
	private final KeyColumnValueStore edgestore;
    private final KeyColumnValueStore propertyIndex;
	
	public DirectStoreMutator(TransactionHandle txh, KeyColumnValueStore edgeStore, KeyColumnValueStore propertyIndex) {
		this.txh = txh;
		this.edgestore = edgeStore;
        this.propertyIndex = propertyIndex;
	}

    @Override
    public void mutateEdges(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) {
        edgestore.mutate(key,additions,deletions,txh);
    }

    @Override
    public void acquireEdgeLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue) {
        edgestore.acquireLock(key,column,expectedValue,txh);
    }

    @Override
    public void mutateIndex(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) {
        propertyIndex.mutate(key,additions,deletions,txh);
    }

    @Override
    public void acquireIndexLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue) {
        propertyIndex.acquireLock(key,column,expectedValue,txh);
    }

    @Override
	public void flush() {
		// Do nothing
	}
}
