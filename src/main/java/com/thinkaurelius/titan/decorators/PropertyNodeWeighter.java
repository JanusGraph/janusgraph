package com.thinkaurelius.titan.decorators;

import com.thinkaurelius.titan.core.Node;
import com.thinkaurelius.titan.core.PropertyType;

public class PropertyNodeWeighter implements NodeWeighter {

	private final PropertyType weight;
	
	public PropertyNodeWeighter(PropertyType weight) {
		this.weight = weight;
	}
	
	@Override
	public double getWeight(Node n) {
		return n.getNumber(weight).doubleValue();
	}

}
