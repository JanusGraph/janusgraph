// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.internal;

import com.google.common.base.Predicate;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.util.datastructures.Retriever;

/**
 * Internal Vertex interface adding methods that should only be used by JanusGraph
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalVertex extends JanusGraphVertex, InternalElement {

    @Override
    InternalVertex it();

    /**
     * Deleted relation e from the adjacency list of this vertex and updates the state of the vertex to reflect
     * the modification.
     * Note that this method tolerates the prior removal of the vertex and hence does not throw an exception
     * if the relation could not actually be removed from the adjacency list. This behavior was chosen to allow
     * relation deletion while iterating over the list of adjacent relations, in which case the relation deletion is taken
     * care of by the iterator and only vertex status update needs to be executed.
     *
     * @param e JanusGraphRelation to be removed
     */
    void removeRelation(InternalRelation e);

    /**
     * Add a new relation to the vertex
     * @param e
     * @return
     */
    boolean addRelation(InternalRelation e);

    /**
     * Returns an iterable over all newly added relations incident on this vertex that match the given predicate
     * @param query
     * @return
     */
    Iterable<InternalRelation> getAddedRelations(Predicate<InternalRelation> query);

    /**
     * Returns all relations that match the given query. If these matching relations are not currently
     * held in memory, it uses the given {@link Retriever} to retrieve the edges from backend storage.
     * @param query
     * @param lookup
     * @return
     */
    EntryList loadRelations(SliceQuery query, Retriever<SliceQuery,EntryList> lookup);

    /**
     * Returns true if the results for the given query have already been loaded for this vertex and are locally cached.
     * @param query
     * @return
     */
    boolean hasLoadedRelations(SliceQuery query);

    /**
     * Whether this vertex has removed relations
     * @return
     */
    boolean hasRemovedRelations();

    /**
     * Whether this vertex has added relations
     * @return
     */
    boolean hasAddedRelations();

    @Override
    VertexCentricQueryBuilder query();


}
