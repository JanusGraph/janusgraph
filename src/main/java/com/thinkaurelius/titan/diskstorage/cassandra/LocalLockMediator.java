package com.thinkaurelius.titan.diskstorage.cassandra;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class LocalLockMediator {
	
	/*
	 * locking-namespace -> mediator.
	 * 
	 * For Cassandra, "locking-namespace" is a column family name.
	 */
	private static final ConcurrentHashMap<String, LocalLockMediator> mediators =
			new ConcurrentHashMap<String, LocalLockMediator>();

	// Locking namespace
	private final String name;
	
	// Lock map
	private final ConcurrentHashMap<Coordinate, Integer> locks =
			new ConcurrentHashMap<Coordinate, Integer>();
	
	private LocalLockMediator(String name) {
		this.name = name;
	}
	
	static LocalLockMediator get(String name) {
		LocalLockMediator m = mediators.get(name);
		
		if (null == m) {
			m = mediators.putIfAbsent(name, new LocalLockMediator(name));
		}
		
		return m;
	}
	
	boolean lock(ByteBuffer key, ByteBuffer column, Integer id) {
		assert null != key;
		assert null != column;
		assert null != id;
		
		Coordinate c = new Coordinate(key, column);
		
		Integer holder = locks.putIfAbsent(c, id);
		
		return null == holder || holder.equals(id);
	}
	
	/*
	 * The id argument isn't strictly necessary.  The id argument's
	 * only use in this method is to let us assert that the lock
	 * being released is indeed held by the supplied id.
	 */
	void unlock(ByteBuffer key, ByteBuffer column, Integer id) {
		
		Coordinate c = new Coordinate(key, column);
		
		assert locks.containsKey(c);
		assert locks.get(c).equals(id);
		
		locks.remove(c);
	}
	
	private static class Coordinate {
		private ByteBuffer key;
		private ByteBuffer column;
		
		private Coordinate(ByteBuffer key, ByteBuffer column) {
			this.key = key;
			this.column = column;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((column == null) ? 0 : column.hashCode());
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			Coordinate other = (Coordinate) obj;
			return other.key.equals(this.key) &&
					other.column.equals(this.column);
		}
		
	}
	
	public String toString() {
		return "[LocalLockMediator:" + name + ":approximately " +
				locks.size() + " active locks]";
	}
}
