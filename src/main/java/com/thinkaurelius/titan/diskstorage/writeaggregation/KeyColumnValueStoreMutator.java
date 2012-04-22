package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;

import java.nio.ByteBuffer;
import java.util.List;

public interface KeyColumnValueStoreMutator {

    /**
     * Applies the specified insertion and deletion mutations to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
	public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions);

    /**
     * Persists any mutation that is currently buffered.
     *
     */
	public void flush();
	
}
