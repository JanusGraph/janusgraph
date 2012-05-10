package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchStoreMutator implements StoreMutator {

	private final TransactionHandle txh;
	private final int maxBatch;
	private final MultiWriteKeyColumnValueStore edgeStore;
    private final MultiWriteKeyColumnValueStore propertyIndex;

    private final Map<ByteBuffer, Mutation> edgeMutations = new HashMap<ByteBuffer, Mutation>();
    private final Map<ByteBuffer, Mutation> indexMutations = new HashMap<ByteBuffer, Mutation>();
    private int numMutations = 0;


	public BatchStoreMutator(TransactionHandle txh,
                             MultiWriteKeyColumnValueStore edgeStore,
                             MultiWriteKeyColumnValueStore propertyIndex,
                             int maxBatch) {
		this.txh = txh;
		this.edgeStore = edgeStore;
        this.propertyIndex = propertyIndex;
		this.maxBatch = maxBatch;
	}

    @Override
    public void mutateEdges(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) {
        mutate(edgeMutations, key, additions, deletions);
    }

    @Override
    public void acquireEdgeLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue) {
        edgeStore.acquireLock(key,column,expectedValue,txh);
    }

    @Override
    public void mutateIndex(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) {
        mutate(indexMutations,key,additions,deletions);
    }

    @Override
    public void acquireIndexLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue) {
        propertyIndex.acquireLock(key,column,expectedValue,txh);
    }

    private void mutate(Map<ByteBuffer, Mutation> mutations, ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) {
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
        
        if (numMutations >= maxBatch) {
            flush();
        }
    }

	@Override
	public void flush() {
        if (numMutations>0) {
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

}
