package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BatchKeyColumnValueStoreMutator
	implements KeyColumnValueStoreMutator {

	private final TransactionHandle txh;
	private final int maxBatch;
	private final MultiWriteKeyColumnValueStore store;


    private final Map<ByteBuffer, Mutation> mutations =
		new HashMap<ByteBuffer, Mutation>();
    private int numMutations = 0;


	public BatchKeyColumnValueStoreMutator(TransactionHandle txh, MultiWriteKeyColumnValueStore store, int maxBatch) {
		this.txh = txh;
		this.store = store;
		this.maxBatch = maxBatch;
	}

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions) {
        if ((additions==null || additions.isEmpty()) && (deletions==null || deletions.isEmpty())) return; 
        
        Mutation m = new Mutation(additions,deletions);
        Mutation existingM = mutations.get(key);
        if (existingM!=null) {
            existingM.merge(m);
        } else {
            mutations.put(key,m);
        }
        
        if (additions!=null) numMutations+= additions.size();
        if (deletions!=null) numMutations+= deletions.size();
        
        if (numMutations >= maxBatch) {
            flush();
        }
    }

	@Override
	public void flush() {
        store.mutateMany(mutations,txh);
        mutations.clear();
        numMutations = 0;
	}
}
