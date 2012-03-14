package com.thinkaurelius.titan.decorators;

import com.thinkaurelius.titan.core.Relationship;


/**
 * Allows custom weightings of relationships in a graph database.
 * 
 * Some graph algorithms require users to provide or will return a weighting of relationships as an implementation of this interface.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 */
public interface RelationshipWeighter {

	/**
	 * Implementation of {@link com.thinkaurelius.titan.decorators.RelationshipWeighter} which returns 1 for each relationship.
	 */
	public final static RelationshipWeighter ConstantOne = new RelationshipWeighter() {
		@Override
		public final double getWeight(Relationship v) {
			return 1;
		}
	};
	
	/**
	 * Returns the weight of the given relationship.
	 * 
	 * @param r Relationship to weigh
	 * @return weight of the relationship
	 */
	public double getWeight(Relationship r);
	
}
