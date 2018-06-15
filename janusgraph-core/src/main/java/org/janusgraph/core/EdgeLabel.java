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


package org.janusgraph.core;

import java.util.Collection;

/**
 * EdgeLabel is an extension of {@link RelationType} for edges. Each edge in JanusGraph has a label.
 * <p>
 * An edge label defines the following characteristics of an edge:
 * <ul>
 * <li><strong>Directionality:</strong> An edge is either directed or unidirected. A directed edge can be thought of
 * as a "normal" edge: it points from one vertex to another and both vertices are aware of the edge's existence. Hence
 * the edge can be traversed in both directions. A unidirected edge is like a hyperlink in that only the out-going
 * vertex is aware of its existence and it can only be traversed in the outgoing direction.</li>
 * <li><strong>Multiplicity:</strong> The multiplicity of an edge imposes restrictions on the number of edges
 * for a particular label that are allowed on a vertex. This allows the definition and enforcement of domain constraints.
 * </li>
 * </ul>
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com)
 * @see RelationType
 */
public interface EdgeLabel extends RelationType {

    /**
     * Checks whether this labels is defined as directed.
     *
     * @return true, if this label is directed, else false.
     */
    boolean isDirected();

    /**
     * Checks whether this labels is defined as unidirected.
     *
     * @return true, if this label is unidirected, else false.
     */
    boolean isUnidirected();

    /**
     * The {@link Multiplicity} for this edge label.
     *
     * @return
     */
    Multiplicity multiplicity();

    /**
     * Collects all property constraints.
     *
     * @return a list of {@link PropertyKey} which represents all property constraints for a {@link EdgeLabel}.
     */
    Collection<PropertyKey> mappedProperties();

    /**
     * Collects all connection constraints.
     *
     * @return a list of {@link Connection} which represents all connection constraints for a {@link EdgeLabel}.
     */
    Collection<Connection> mappedConnections();

}
