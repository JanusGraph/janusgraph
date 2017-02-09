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

import org.janusgraph.core.schema.JanusGraphSchemaType;

/**
 * A vertex label is a label attached to vertices in a JanusGraph graph. This can be used to define the nature of a
 * vertex.
 * <p />
 * Internally, a vertex label is also used to specify certain characteristics of vertices that have a given label.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexLabel extends JanusGraphVertex, JanusGraphSchemaType {

    /**
     * Whether vertices with this label are partitioned.
     *
     * @return
     */
    public boolean isPartitioned();

    /**
     * Whether vertices with this label are static, that is, immutable beyond the transaction
     * in which they were created.
     *
     * @return
     */
    public boolean isStatic();

    //TTL


}
