package com.thinkaurelius.titan.graphdb.query;

import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.util.interval.AtomicInterval;

import java.util.Map;

public class QueryUtil {

	public static final TitanProperty queryHiddenFunctionalProperty(InternalTitanVertex node, TitanKey propType) {
		assert ((InternalTitanType)propType).isHidden() : "Expected hidden property key";
		assert propType.isFunctional() : "Expected functional property  type";
		return Iterators.getOnlyElement(
				new SimpleAtomicQuery(node).
					includeHidden().
                        type(propType).
                        propertyIterator(),null);
	}

    /**
     * Checks whether the query can be answered by disk indexes alone or whether further processing in memory is necessary
     * to determine the exact result set.
     *
     * @param query Query to check
     * @return
     */
    public static boolean queryCoveredByDiskIndexes(AtomicQuery query) {
        if (!query.hasConstraints()) return true;
        if (!query.hasEdgeTypeCondition()) return false;
        String[] keysig = ((InternalTitanType)query.getTypeCondition()).getDefinition().getKeySignature();
        Map<String,Object> constraints = query.getConstraints();
        int num = 0;
        for (String key : keysig) {
            if (!constraints.containsKey(key)) break;
            Object o = constraints.get(key);
            num++;
            if (o!=null && (o instanceof AtomicInterval) && ((AtomicInterval) o).isRange()) {
                if (((AtomicInterval)o).hasHoles()) num--; //Intervals with holes have to be filtered in memory
                break;
            }
        }
        assert num<=constraints.size();
        return num==constraints.size();
    }

    /**
     * Whether the given query can be answered by exploiting a primary key index. This is true if the query asks for edges
     * of oen particular type, has edge constraints and those edge constraints are covered by the primary key on the edge type.
     * 
     * If this method returns true, then the query answering engine can answer the query more selectively and does not have
     * to retrieve all incident edges of the type.
     * This is useful to know, for instance, when determining whether a particular query has loaded all edges, as in {@link com.thinkaurelius.titan.graphdb.loadingstatus.BasicLoadingStatus}
     * 
     * @param query The query to check
     * @return
     */
    public static boolean hasFirstKeyConstraint(AtomicQuery query) {
        if (!query.hasConstraints()) return false;
        if (!query.hasEdgeTypeCondition()) return false;
        String[] keysig = ((InternalTitanType)query.getTypeCondition()).getDefinition().getKeySignature();
        return keysig.length>0 && query.getConstraints().containsKey(keysig[0]);
    }
    
}
