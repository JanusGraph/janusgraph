
package com.thinkaurelius.titan.core;

/**
 * Enumerates the four possible edge categories which is the primary characteristic of an edge and defined via
 * its {@link EdgeType}:
 * <ul>
 * <li>{@link #Simple}: unlabeled and most memory efficient</li>
 * <li>{@link #LabeledRestricted}: only allows certain properties as labels, memory efficient</li>
 * <li>{@link #Labeled}: allows arbitrary properties as labels</li>
 * </ul>
 * 
 * The edge type category is defined when the edge type is created and used to select the memory representation and
 * associated data structures for each edge of that edge type.
 * 
 * @author	Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public enum EdgeCategory {

	/**
	 * A simple edge is unlabeled and therefore does not allow incident properties or relationships.
	 * Simple edges are the most memory efficient and provide a performance advantage when it is 
	 * known a priori that edges of a given type will not have labels.
	 */
	Simple,
	
	/**
	 * A label-restricted edge can be labeled with properties but only allows properties
	 * for a set of functional property types which is defined at the time this property type is created.
	 * In other words, a label-restricted edge can only be assigned properties from a restricted set of 
	 * property types which must be known at the time the edge type is created. 
	 * Label-restricted edges are more memory efficient than labeled edges.
	 */
	LabeledRestricted,
	
	/**
	 * A labeled edge can be labeled with arbitrary properties.
	 */
	Labeled;
	
	/**
	 * Checks whether this edge type category allows labels (i.e. incident properties) on the edges.
	 * All but {@link #Simple} allow edge labels.
	 * 
	 * @return true if the category allows labels, else false.
	 */
	public boolean isLabeled() {
		switch(this) {
		case Simple: return false;
		case Labeled: 
		case LabeledRestricted: return true;
		default: throw new AssertionError("Unexpected enum constant: " + this);
		}
	}
	
}
