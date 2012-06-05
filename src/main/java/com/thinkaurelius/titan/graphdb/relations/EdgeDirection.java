package com.thinkaurelius.titan.graphdb.relations;

import com.tinkerpop.blueprints.Direction;

/**
 * IMPORTANT: The byte values of the proper directions must be sequential, 
 * i.e. the byte values of proper and improper directions may NOT be mixed.
 * This is crucial IN the retrieval for proper edges where we make this assumption.
 * 
 * 
 * @author Matthias Broecheler (me@matthiasb.com);
 *
 */
public enum EdgeDirection {
	
	OUT {
		public final byte getID() {
			return 1;
		}
        
        public boolean impliedBy(Direction dir) {
            return dir==Direction.OUT || dir == Direction.BOTH;
        }
	}, 
	
	IN {
		public final byte getID() {
			return 2;
		}

        public boolean impliedBy(Direction dir) {
            return dir==Direction.IN || dir == Direction.BOTH;
        }
	};
	

	public abstract byte getID();

	public abstract boolean impliedBy(Direction dir);
    
    public final static EdgeDirection fromID(long dir) {
		return fromID((int)dir);
	}
	
    
    
	public final static EdgeDirection fromID(int dir) {
		switch(dir) {
		case 1: return OUT;
		case 2: return IN;
		default: throw new IllegalArgumentException("Unkown edge direction");
		}
	}
	
	public final static EdgeDirection convert(Direction dir) {
		switch(dir) {
		case IN: return IN;
		case OUT: return OUT;
		default: throw new IllegalArgumentException("Unsupported Direction: "+ dir);
		}
	}
	
	public final static Direction convert(EdgeDirection dir) {
		switch(dir) {
		case IN: return Direction.IN;
		case OUT: return Direction.OUT;
		default: throw new IllegalArgumentException("Unsupported Direction: "+ dir);
		}
	}
	
	
}
