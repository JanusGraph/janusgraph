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

package org.janusgraph.graphdb.transaction.vertexcache;

import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.util.datastructures.Retriever;

import java.util.List;

public interface VertexCache {

    /**
     * Checks whether the cache contains a vertex with the given id
     *
     * @param id Vertex id
     * @return true if a vertex with the given id is contained, else false
     */
    boolean contains(long id);

    /**
     * Returns the vertex with the given id or null if it is not in the cache
     *
     * @param id
     * @return
     */
    InternalVertex get(long id, Retriever<Long, InternalVertex> retriever);

    /**
     * Adds the given vertex with the given id to the cache. The given vertex may already be in the cache.
     * In other words, this method may be called to ensure that a vertex is still in the cache.
     *
     * @param vertex
     * @param id
     * @throws IllegalArgumentException if the vertex is null or the id negative
     */
    void add(InternalVertex vertex, long id);

    /**
     * Returns an iterable over all new vertices in the cache
     *
     * @return
     */
    List<InternalVertex> getAllNew();

    /**
     * Closes the cache which allows the cache to release allocated memory.
     * Calling any of the other methods after closing a cache has undetermined behavior.
     */
    void close();

}
