package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.base.Predicate;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.util.datastructures.Retriever;

import java.util.List;


public interface InternalVertex extends TitanVertex, InternalElement {

    public InternalVertex it();

	/* ---------------------------------------------------------------
	 * Incident TitanRelation Operations - Package level only
	 * ---------------------------------------------------------------
	 */


    /**
     * Deleted edge e from the adjacency list of this node and updates the state of the node to reflect
     * the modification.
     * Note that this method tolerates the prior removal of the node and hence does not throw an exception
     * if the edge could not actually be removed from the adjacency list. This behavior was chosen to allow
     * edge deletion while iterating over the list of adjacent edges, in which case the edge deletion is taken
     * care of by the iterator and only node status update needs to be executed.
     *
     * @param e TitanRelation to be removed
     */
    public void removeRelation(InternalRelation e);

    public boolean addRelation(InternalRelation e);


    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query);

    public Iterable<Entry> loadRelations(SliceQuery query, Retriever<SliceQuery,List<Entry>> lookup);


    public boolean hasRemovedRelations();

    public boolean hasAddedRelations();


}
