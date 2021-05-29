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

package org.janusgraph.core.log;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.RelationType;

import java.util.Set;

/**
 * Container interface for a set of changes against the graph caused by a particular transaction. This is passed as an argument to
 * {@link ChangeProcessor#process(org.janusgraph.core.JanusGraphTransaction, TransactionId, ChangeState)}
 * for the user to retrieve changed elements and act upon it.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ChangeState {

    /**
     * Returns all added, removed, or modified vertices when the change argument is {@link Change#ADDED},
     * {@link Change#REMOVED}, or {@link Change#ANY} respectively.
     *
     * @param change
     * @return
     */
    Set<JanusGraphVertex> getVertices(Change change);

    /**
     * Returns all relations that match the change state and any of the provided relation types. If no relation types
     * are specified all relations matching the state are returned.
     *
     * @param change
     * @param types
     * @return
     */
    Iterable<JanusGraphRelation> getRelations(Change change, RelationType... types);

    /**
     * Returns all edges incident on the given vertex in the given direction that match the provided change state and edge labels.
     *
     * @param vertex
     * @param change
     * @param dir
     * @param labels
     * @return
     */
    Iterable<JanusGraphEdge> getEdges(Vertex vertex, Change change, Direction dir, String... labels);


    /**
     * Returns all properties incident for the given vertex that match the provided change state and property keys.
     *
     * @param vertex
     * @param change
     * @param keys
     * @return
     */
    Iterable<JanusGraphVertexProperty> getProperties(Vertex vertex, Change change, String... keys);



}
