package com.thinkaurelius.titan.graphdb.edgequery;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.Property;
import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.vertices.InternalNode;

import java.util.Map;

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


    public static boolean allConstraintsKeyed(InternalEdgeQuery query) {
        if (!query.hasConstraints()) return true;
        if (!query.hasEdgeTypeGroupCondition()) return false;
        String[] keysig = ((InternalEdgeType)query.getEdgeTypeCondition()).getDefinition().getKeySignature();
        Map<String,Object> constraints = query.getConstraints();
        int num = 0;
        for (String key : keysig) {
            if (!constraints.containsKey(key)) break;
            Object o = constraints.get(key);
            num++;
            if (o!=null && (o instanceof Interval) && ((Interval) o).isRange()) break;
        }
        assert num<=constraints.size();
        return num==constraints.size();
    }
    
    public static boolean hasFirstKeyConstraint(InternalEdgeQuery query) {
        if (!query.hasConstraints()) return false;
        if (!query.hasEdgeTypeGroupCondition()) return false;
        String[] keysig = ((InternalEdgeType)query.getEdgeTypeCondition()).getDefinition().getKeySignature();
        return query.getConstraints().containsKey(keysig[0]);
    }
    
}
