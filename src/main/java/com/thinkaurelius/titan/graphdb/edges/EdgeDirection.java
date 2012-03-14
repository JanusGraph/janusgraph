package com.thinkaurelius.titan.graphdb.edges;

import com.thinkaurelius.titan.core.Direction;

/**
 * IMPORTANT: The byte values of the proper directions must be sequential, 
 * i.e. the byte values of proper and improper directions may NOT be mixed.
 * This is crucial in the retrieval for proper edges where we make this assumption.
 * 
 * 
 * @author Matthias Broecheler (me@matthiasb.com);
 *
 */
public enum EdgeDirection {

	Undirected {
		public final byte getID() {
			return 0;
		}
	},
	
	Out {
		public final byte getID() {
			return 1;
		}
	}, 
	
	In {
		public final byte getID() {
			return 2;
		}
	};
	

	public abstract byte getID();
	
	public static final EdgeDirection smallestProperDirection() {
		return Undirected;
	}
	
	public final static EdgeDirection fromID(long dir) {
		return fromID((int)dir);
	}
	
	public final static EdgeDirection fromID(int dir) {
		switch(dir) {
		case 1: return Out;
		case 0: return Undirected;
		case 2: return In;
		default: throw new IllegalArgumentException("Unkown edge direction!");
		}
	}
	
	public final static EdgeDirection convert(Direction dir) {
		switch(dir) {
		case In: return In;
		case Out: return Out;
		case Undirected: return Undirected;
		default: throw new IllegalArgumentException("Unsupported Direction: "+ dir);
		}
	}
	
	public final static Direction convert(EdgeDirection dir) {
		switch(dir) {
		case In: return Direction.In;
		case Out: return Direction.Out;
		case Undirected: return Direction.Undirected;
		default: throw new IllegalArgumentException("Unsupported Direction: "+ dir);
		}
	}
	
	
}
