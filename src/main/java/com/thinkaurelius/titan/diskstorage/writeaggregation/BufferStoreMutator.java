package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferStoreMutator implements StoreMutator {

	protected final TransactionHandle txh;
    protected final int bufferSize;
    protected final MultiWriteKeyColumnValueStore edgeStore;
    protected final MultiWriteKeyColumnValueStore propertyIndex;

    protected Map<ByteBuffer, Mutation> edgeMutations = new HashMap<ByteBuffer, Mutation>();
    protected Map<ByteBuffer, Mutation> indexMutations = new HashMap<ByteBuffer, Mutation>();
    protected int numMutations = 0;


	public BufferStoreMutator(TransactionHandle txh,
                              MultiWriteKeyColumnValueStore edgeStore,
                              MultiWriteKeyColumnValueStore propertyIndex,
                              int bufferSize) {
		this.txh = txh;
		this.edgeStore = edgeStore;
        this.propertyIndex = propertyIndex;
		this.bufferSize = bufferSize;
	}

    @Override
    public void mutateEdges(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) throws StorageException {
        mutate(edgeMutations, key, additions, deletions);
    }

    @Override
    public void acquireEdgeLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue) throws StorageException {
        edgeStore.acquireLock(key,column,expectedValue,txh);
    }

    @Override
    public void mutateIndex(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) throws StorageException {
        mutate(indexMutations,key,additions,deletions);
    }

    @Override
    public void acquireIndexLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue) throws StorageException {
        propertyIndex.acquireLock(key,column,expectedValue,txh);
    }

    private void mutate(Map<ByteBuffer, Mutation> mutations, ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) throws StorageException {
        if ((additions==null || additions.isEmpty()) && (deletions==null || deletions.isEmpty())) return; 
        
        Mutation m = new Mutation(additions,deletions);
        Mutation existingM = mutations.get(key);
        if (existingM!=null) {
            existingM.merge(m);
        } else {
            mutations.put(key, m);
        }
        
        if (additions!=null) numMutations+= additions.size();
        if (deletions!=null) numMutations+= deletions.size();
        
        if (numMutations >= bufferSize) {
            flushInternal();
        }
    }

	@Override
	public void flush()  throws StorageException{
        if (numMutations>0) {
            flushInternal();
        }
	}

    protected void flushInternal() throws StorageException {
        if (!edgeMutations.isEmpty()) {
            edgeStore.mutateMany(edgeMutations,txh);
            edgeMutations.clear();
        }

        if (!indexMutations.isEmpty()) {
            propertyIndex.mutateMany(indexMutations,txh);
            indexMutations.clear();
        }

        numMutations = 0;
    }

}
