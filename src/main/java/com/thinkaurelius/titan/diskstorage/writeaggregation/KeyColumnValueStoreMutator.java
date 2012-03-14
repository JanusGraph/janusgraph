package com.thinkaurelius.titan.diskstorage.writeaggregation;

import com.thinkaurelius.titan.diskstorage.Entry;

import java.nio.ByteBuffer;
import java.util.List;

public interface KeyColumnValueStoreMutator {

	public void insert(ByteBuffer key, List<Entry> entries);
	public void delete(ByteBuffer key, List<ByteBuffer> columns);
	
	public void flushInserts();
	public void flushDeletes();
	
	public void flush();
	
}
