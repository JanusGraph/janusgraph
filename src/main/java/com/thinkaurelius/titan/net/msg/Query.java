package com.thinkaurelius.titan.net.msg;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * Represents a Query instance.
 *
 * @author dalaro
 */
public class Query extends Message {

    /** Numeric identifier representing the Stored Procedure this query uses */
    private final int type;
    /** Identifies the originator of this query (the "tracker").
        This field does not change when the query is forwarded by a
        worker node, possibly with different data. */
    private final Key seed;
    /** Identifies this unit of work in the larger query identified by seed.
        This field changes every time the query is forwarded by a worker node.
        The worker node forms the key from its own address and a monotonically
        increasing counter, guaranteeing that all tags are globally unique. */
    private final Key instance;
    /** Approximate age of the query.  If a stored procedure, in the process of
        executing this instance, creates a new instance with the same seed,
        then the new instance's generation will be the existing intstance's
        generation plus one. */
    private final int generation;
    /** Flags that alter how the cluster handles the query  @see Mode */
    private final EnumSet<Mode> modes;
    /** Query body (opaque except to the stored proc) */
    private final ByteBuffer[] data;
    /** Query start node (provided by query in GraphTransaction.forwardQuery() call) */
    private final Long nodeId;

    public static final int INITIAL_GENERATION = -1;

    private static final EnumSet<Mode> defaultModes = EnumSet.of(Mode.TRACE);

    public Query(Key seed, Key instance, int generation, int type,
            ByteBuffer[] data, EnumSet<Mode> modes) {
    	this(seed, instance, generation, type, data, modes, null);
    }
    
    public Query(Key seed, Key instance, int generation, int type,
            ByteBuffer[] data, EnumSet<Mode> modes, Long nodeId) {
        this.seed = seed;
        this.instance = instance;
        this.generation = generation;
        this.type = type;
        this.modes = modes;
        this.data = data;
        this.nodeId = nodeId;
    }

    public Query(Key key, int type, long nodeId, ByteBuffer[] data) {
        this(key, key, INITIAL_GENERATION, type, data, defaultModes, nodeId);
    }

//    public Query(long timeStamp, InetSocketAddress client, int type, ByteBuffer[] data) {
//        this(timeStamp, client, type, data, null);
//    }

    public InetSocketAddress getClient() {
        return getSeed().getHost();
    }

    public Key getSeed() {
        return seed;
    }

    public Key getInstance() {
        return instance;
    }
    
    public int getType() {
    	return type;
    }

    public ByteBuffer[] getData() {
        return data;
    }
    
    public int getGeneration() {
    	return generation;
    }
    
    public Long getNodeId() {
    	return nodeId;
    }
    
    public EnumSet<Mode> getMode() {
    	return EnumSet.copyOf(modes);
    }

    public boolean isTracingEnabled() {
        return modes.contains(Mode.TRACE);
    }

    public boolean getToleratePartialResult() {
        return modes.contains(Mode.TOLERATE_PARTIAL_RESULT);
    }

    public boolean getTolerateDuplicateResult() {
        return modes.contains(Mode.TOLERATE_DUPLICATE_RESULT);
    }

    public int getQueryType() {
        return type;
    }
    
    public Query spawnNextGeneration(Key instance, ByteBuffer data[], long nodeId) {
    	return new Query(seed, instance, generation+1, type, data, getMode(), nodeId);
    }

    public static enum Mode {
        /**
         * This mode governs the system's response when a storage node
         * required to answer the query is unreachable.
         *
         * - When set: continue processing the query
         * - When unset: globally cancel the query immediately
         */
        TOLERATE_PARTIAL_RESULT,
        /**
         * This mode governs the system's response when the first replica
         * holding a certain chunk of the graph silently drops queries (fails
         * to send acknowledgment of any kind)
         *
         * - When set: globally cancel the query immediately
         * - When unset: try the other replicas
         */
        TOLERATE_DUPLICATE_RESULT,
        /**
         * This mode makes worker nodes send Trace messages to the tracker
         * at a few points during query processing.  It is intended for
         * debugging only.
         */
        TRACE;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + generation;
		result = prime * result
				+ ((instance == null) ? 0 : instance.hashCode());
		result = prime * result + ((modes == null) ? 0 : modes.hashCode());
		result = prime * result + ((seed == null) ? 0 : seed.hashCode());
		result = prime * result + type;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Query other = (Query) obj;
		if (generation != other.generation)
			return false;
		if (instance == null) {
			if (other.instance != null)
				return false;
		} else if (!instance.equals(other.instance))
			return false;
		if (modes == null) {
			if (other.modes != null)
				return false;
		} else if (!modes.equals(other.modes))
			return false;
		if (seed == null) {
			if (other.seed != null)
				return false;
		} else if (!seed.equals(other.seed))
			return false;
		if (type != other.type)
			return false;
		return true;
	};
    
    

}
