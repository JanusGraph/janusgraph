package com.thinkaurelius.titan.decorators;

import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.core.Relationship;

public class PropertyRelationshipWeighter implements RelationshipWeighter {

	private final PropertyType weight;
	
	public PropertyRelationshipWeighter(PropertyType weight) {
		this.weight = weight;
	}
	
	@Override
	public double getWeight(Relationship r) {
		return r.getNumber(weight).doubleValue();
	}

}
