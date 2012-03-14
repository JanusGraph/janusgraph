
package com.thinkaurelius.titan.core;

/**
 * Enumerates the two possible positions of a node with
 * respect to a given incident edge: {@link #Start} and {@link #End}.
 * In other words, a node can be the start node or end node of an edge.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public enum NodePosition {
	
	/**
	 * Denotes the start position of a node for an incident edge.
	 */
	Start {
		@Override
		public NodePosition opposite() {
			return End;
		}
		@Override
		public int position() {
			return 0;
		}
	},
	
	/**
	 * Denotes the end position of a node for an incident edge.
	 */
	End {
		@Override
		public NodePosition opposite() {
			return Start;
		}
		@Override
		public int position() {
			return 1;
		}
	};
	
	/**
	 * Returns the opposite of this node position.
	 * 
	 * @return The opposite position to this position.
	 */
	public abstract NodePosition opposite();
	
	/**
	 * Returns a unique numeric value for the position.
	 * 
	 * @return numeric value representing the position.
	 */
	public abstract int position();
	
	/**
	 * Returns the node position constant for the specified numeric value
	 * 
	 * @param position Numeric value of the node position
	 * @return Node position corresponding to the specified value
	 */
	public static NodePosition getPosition(int position) {
		switch(position) {
		case 0: return NodePosition.Start;
		case 1: return NodePosition.End;
		default: throw new IllegalArgumentException("NodePositions are only valid for position values 0 and 1!");
		}
	}

}
