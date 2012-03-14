package com.thinkaurelius.titan.graphdb.edgequery;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.Property;
import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

public class EdgeQueryUtil {

	public static final Property queryHiddenFunctionalProperty(InternalNode node, PropertyType propType) {
		assert propType.isHidden() : "Expected hidden property type";
		assert propType.isFunctional() : "Expected functional property  type";
		return Iterators.getOnlyElement(
				new StandardEdgeQuery(node).
					includeHidden().
					withEdgeType(propType).
					getPropertyIterator(),null);
	}
	
}
