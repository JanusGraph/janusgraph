package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.exceptions.QueryException;
import com.thinkaurelius.titan.graphdb.adjacencylist.AdjacencyList;
import com.thinkaurelius.titan.graphdb.edgequery.InternalEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.InternalEdge;

public class NodeUtil {

	public static void checkAccessbility(InternalNode v) {
		Preconditions.checkArgument(v.isAccessible(),"Node is not accessible!");
	}
	
	public static void checkAvailability(InternalNode v) {
		Preconditions.checkArgument(v.isAvailable(),"Node is not available!");
	}
	
	public static Iterable<InternalEdge> filterQueryQualifications(final InternalEdgeQuery query, Iterable<InternalEdge> iter ) {
		if (iter==AdjacencyList.Empty) return iter;
		
		if (query.queryHidden() && query.queryUnmodifiable() && query.queryProperties()
				&& query.queryRelationships()) return iter;
		if (!query.queryProperties() && !query.queryRelationships()) 
			throw new QueryException("Query excludes both: properties and relationships");
		
		
		return Iterables.filter(iter,  new Predicate<InternalEdge>(){

				@Override
				public boolean apply(InternalEdge e) {
					if (!query.queryProperties() && e.isProperty()) return false;
					if (!query.queryRelationships() && e.isRelationship()) return false;
					if (!query.queryHidden() && e.isHidden()) return false;
					if (!query.queryUnmodifiable() && !e.isModifiable()) return false;
					return true;
				}
				
		});

	}
	
	
	public static final Iterable<InternalEdge> filterLoopEdges(Iterable<InternalEdge> iter, final InternalNode v) {
		if (iter==AdjacencyList.Empty) return iter;		
		else return Iterables.filter(iter,new Predicate<InternalEdge>(){

			@Override
			public boolean apply(InternalEdge edge) {
				if (edge.isSelfLoop(v)) return false;
				else return true;
			}}
		);
		
	}


    public static final Iterable<InternalEdge> getQuerySpecificIterable(AdjacencyList edges, InternalEdgeQuery query) {
        if (query.hasEdgeTypeCondition()) {
            assert query.getEdgeTypeCondition()!=null;
            return edges.getEdges(query.getEdgeTypeCondition());
        } else if (query.hasEdgeTypeGroupCondition()) {
            return edges.getEdges(query.getEdgeTypeGroupCondition());
        } else {
            return edges.getEdges();
        }
    }

	
	/**
	 * Checks whether the two given vertices have the same id(s).
	 * Note that this method assumes that both v1,v2 are not NULL.
	 * 
	 * @param v1
	 * @param v2
	 * @return
	 */
	public static final boolean equalIDs(InternalNode v1, InternalNode v2) {
		if (v1.hasID() && v2.hasID()) {
			return v1.getID()==v2.getID();
		} else return false;
	}
	
	public static final int getIDHashCode(InternalNode v1) {
		if (v1.hasID()) {
			long id = v1.getID();
			return 37*31 + (int)(id ^ (id >>>32));
		} else throw new IllegalArgumentException("Given node does not have an ID!");
	}


}
