package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.base.Predicate;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.query.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.util.datastructures.Retriever;

import java.util.Collection;
import java.util.List;

/**
 * Internal Vertex interface adding methods that should only be used by Titan
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalVertex extends TitanVertex, InternalElement {

    public InternalVertex it();

	/* ---------------------------------------------------------------
	 * Incident TitanRelation Operations - Package level only
	 * ---------------------------------------------------------------
	 */


    /**
     * Deleted relation e from the adjacency list of this vertex and updates the state of the vertex to reflect
     * the modification.
     * Note that this method tolerates the prior removal of the vertex and hence does not throw an exception
     * if the relation could not actually be removed from the adjacency list. This behavior was chosen to allow
     * relation deletion while iterating over the list of adjacent relations, in which case the relation deletion is taken
     * care of by the iterator and only vertex status update needs to be executed.
     *
     * @param e TitanRelation to be removed
     */
    public void removeRelation(InternalRelation e);

    /**
     * Add a new relation to the vertex
     * @param e
     * @return
     */
    public boolean addRelation(InternalRelation e);

    /**
     * Returns an iterable over all newly added relations incident on this vertex that match the given predicate
     * @param query
     * @return
     */
    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query);

    /**
     * Returns all relations that match the given query. If these matching relations are not currently
     * held in memory, it uses the given {@link Retriever} to retrieve the edges from backend storage.
     * @param query
     * @param lookup
     * @return
     */
    public Collection<Entry> loadRelations(SliceQuery query, Retriever<SliceQuery,List<Entry>> lookup);

    /**
     * Returns true if the results for the given query have already been loaded for this vertex and are locally cached.
     * @param query
     * @return
     */
    public boolean hasLoadedRelations(SliceQuery query);

    /**
     * Whether this vertex has removed relations
     * @return
     */
    public boolean hasRemovedRelations();

    /**
     * Whether this vertex has added relations
     * @return
     */
    public boolean hasAddedRelations();

    public VertexCentricQueryBuilder query();


}
