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

package org.janusgraph.graphdb.tinkerpop.optimize.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface MultiQueriable<S,E> extends Step<S,E> {

    void setUseMultiQuery(boolean useMultiQuery);

    /**
     * Registers a vertex which will pass this step at some point in the future.
     * The vertex is typically known because a traverser at that vertex location
     * has passed a previous step earlier.
     * Using that information, a step can know in advance a set of vertices which
     * it will have to handle in the future.
     * @param futureVertex The vertex which will reach the step in the future.
     */
    void registerFutureVertexForPrefetching(Vertex futureVertex);
}
