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

package org.janusgraph.core.schema;

/**
 * A JanusGraphSchemaType is a {@link JanusGraphSchemaElement} that represents a label or key
 * used in the graph. As such, a schema type is either a {@link org.janusgraph.core.RelationType}
 * or a {@link org.janusgraph.core.VertexLabel}.
 * <p>
 * JanusGraphSchemaTypes are a special {@link JanusGraphSchemaElement} in that they are referenced from the
 * main graph when creating vertices, edges, and properties.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusGraphSchemaType extends JanusGraphSchemaElement {

    /**
     * Checks whether this schema type has been newly created in the current transaction.
     *
     * @return True, if the schema type has been newly created, else false.
     */
    boolean isNew();
}
