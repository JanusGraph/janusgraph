// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.transaction;

/**
 * What a lookup of vertices by id reads of each vertex along with its existence, in the same backend read. A traversal
 * which looks vertices up by id ({@code g.V(ids)}) reads what is read of each vertex right after the lookup anyway, see
 * {@link org.janusgraph.graphdb.tinkerpop.optimize.step.util.VertexLookupUtil}; every other lookup reads the existence
 * alone. The cells read along with the existence are at most
 * {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#FAST_PROPERTY_LOOKUP_LIMIT}.
 */
public enum VertexLookup {

    /**
     * The existence marker alone.
     */
    EXISTENCE,

    /**
     * The existence marker and the other system relations of the vertex, its label among them.
     */
    LABEL,

    /**
     * The existence marker, the label and the properties of the vertex, which are stored before its edges.
     */
    LABEL_AND_PROPERTIES
}
