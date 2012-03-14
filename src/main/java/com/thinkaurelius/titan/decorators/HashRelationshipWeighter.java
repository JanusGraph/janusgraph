package com.thinkaurelius.titan.decorators;

import com.thinkaurelius.titan.core.Relationship;

import java.util.HashMap;
import java.util.Map;

public class HashRelationshipWeighter implements RelationshipWeighter {

	private final double defaultweight;
	private final Map<Relationship,Double> weights;
	
	public HashRelationshipWeighter() {
		this(Double.NaN);
	}
	
	public HashRelationshipWeighter(double defaultWeight) {
		this.defaultweight=defaultWeight;
		weights = new HashMap<Relationship,Double>();
	}
	
	public void setWeight(Relationship r, double w) {
		weights.put(r, Double.valueOf(w));
	}
	
	@Override
	public double getWeight(Relationship r) {
		Double w = weights.get(r);
		if (w==null) {
			if (Double.isNaN(defaultweight))
				throw new IllegalArgumentException("No weight has been specified for the given relationship: " + r);
			else 
				return defaultweight;
		} else {
			return w.doubleValue();
		}
	}

	
	
}
