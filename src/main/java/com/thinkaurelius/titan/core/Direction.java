
package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.edges.EdgeDirection;

/**
 * Enumerates all possible directions of an {@link Edge} from the perspective of an incident {@link Node}.
 * {@link #In}: Incoming {@link Edge}, i.e. the edge originates from the node.
 * {@link #Out}: Outgoing {@link Edge}, i.e. the edge terminates at the node.
 * {@link #Undirected}: Undirected {@link Edge} for which there exists no start or end node as all incident nodes are undistinguishable.
 * {@link #Both}: Constant which represents both directions: incoming and outgoing. Useful in formulating query but not a proper direction itself.
 * 
 * Note that only the first three directions are proper {@link Edge} directions whereas the last one subsumes both {@link #In} and {@link #Out}.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */

public enum Direction
{

	/**
	 * Represents the direction of incoming edges from the perspective of a given node. This is a proper direction.
	 */
	In {
		@Override
		public boolean implies(EdgeDirection dir) {
			return dir==EdgeDirection.In;
		}


		@Override
		public boolean isProper() {return true;}
	}, 
	
	/**
	 * Represents the direction of outgoing edges from the perspective of a given node. This is a proper direction.
	 */
	Out {
		@Override
		public boolean implies(EdgeDirection dir) {
			return dir==EdgeDirection.Out;
		}

		@Override
		public boolean isProper() {return true;}
	}, 
	
	/**
	 * Represents both incoming and outgoing edges. This is an improper direction.
	 */
	Both {
		@Override
		public boolean implies(EdgeDirection dir) {
			return dir==EdgeDirection.In || dir==EdgeDirection.Out;
		}

		@Override
		public boolean isProper() {return false;}
	}, 
	
	/**
	 * Represents the direction of an undirected edge. This is a proper direction.
	 */
	Undirected {
		@Override
		public boolean implies(EdgeDirection dir) {
			return dir==EdgeDirection.Undirected;
		}

		@Override
		public boolean isProper() {return true;}

	};
	
	/**
	 * Defines all proper {@link Edge} directions: {@link #In}, {@link #Out}, {@link #Undirected}.
	 */
	public static final Direction[] properDirections = new Direction[]{Out,In,Undirected};
	
	/**
	 * Indicates whether some other direction is implied or subsumed by this one.
	 * 
	 * All proper directions only imply or subsume themselves. {@link #Both} implies {@link #In} and {@link #Out}.
	 * 
	 * @param dir The reference direction for which subsumption is checked.
	 * @return true if the reference direction is subsumed, else false.
	 */
	public abstract boolean implies(EdgeDirection dir);
	
	/**
	 * Indicates whether this direction is proper.
	 * 
	 * Proper directions are: {@link #In}, {@link #Out}, {@link #Undirected}.
	 * Improper directions are: {@link #Both}
	 * @return true if this direction is proper, else false.
	 */
	public abstract boolean isProper();
	

	/* ---------------------------------------------------------------
	 * EdgeQuery Implementation
	 * ---------------------------------------------------------------
	 */
	
//	@Override
//	public EdgeType getEdgeTypeCondition() {
//		throw new IllegalStateException("This query does not have a edge type condition!");
//	}
//
//	@Override
//	public boolean hasDirectionCondition() {
//		return true;
//	}
//
//	@Override
//	public boolean hasEdgeTypeCondition() {
//		return false;
//	}
//
//	@Override
	public boolean isAllowedDirection(EdgeDirection dir) {
		return this.implies(dir);
	}

	
}
