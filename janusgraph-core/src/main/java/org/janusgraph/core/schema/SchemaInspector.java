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

import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface SchemaInspector {

   /* ---------------------------------------------------------------
    * Schema
    * ---------------------------------------------------------------
    */

    /**
     * Checks whether a type with the specified name exists.
     *
     * @param name name of the type
     * @return true, if a type with the given name exists, else false
     */
    public boolean containsRelationType(String name);

    /**
     * Returns the type with the given name.
     * Note, that type names must be unique.
     *
     * @param name name of the type to return
     * @return The type with the given name, or null if such does not exist
     * @see RelationType
     */
    public RelationType getRelationType(String name);

    /**
     * Checks whether a property key of the given name has been defined in the JanusGraph schema.
     *
     * @param name name of the property key
     * @return true, if the property key exists, else false
     */
    public boolean containsPropertyKey(String name);

    /**
     * Returns the property key with the given name. If automatic type making is enabled, it will make the property key
     * using the configured default type maker if a key with the given name does not exist.
     *
     * @param name name of the property key to return
     * @return the property key with the given name
     * @throws IllegalArgumentException if a property key with the given name does not exist or if the
     *                                  type with the given name is not a property key
     * @see PropertyKey
     */
    public PropertyKey getOrCreatePropertyKey(String name);

    /**
     * Returns the property key with the given name. If it does not exist, NULL is returned
     *
     * @param name
     * @return
     */
    public PropertyKey getPropertyKey(String name);

    /**
     * Checks whether an edge label of the given name has been defined in the JanusGraph schema.
     *
     * @param name name of the edge label
     * @return true, if the edge label exists, else false
     */
    public boolean containsEdgeLabel(String name);

    /**
     * Returns the edge label with the given name. If automatic type making is enabled, it will make the edge label
     * using the configured default type maker if a label with the given name does not exist.
     *
     * @param name name of the edge label to return
     * @return the edge label with the given name
     * @throws IllegalArgumentException if an edge label with the given name does not exist or if the
     *                                  type with the given name is not an edge label
     * @see EdgeLabel
     */
    public EdgeLabel getOrCreateEdgeLabel(String name);

    /**
     * Returns the edge label with the given name. If it does not exist, NULL is returned
     * @param name
     * @return
     */
    public EdgeLabel getEdgeLabel(String name);

    /**
     * Whether a vertex label with the given name exists in the graph.
     *
     * @param name
     * @return
     */
    public boolean containsVertexLabel(String name);

    /**
     * Returns the vertex label with the given name. If such does not exist, NULL is returned.
     *
     * @param name
     * @return
     */
    public VertexLabel getVertexLabel(String name);

    /**
     * Returns the vertex label with the given name. If a vertex label with this name does not exist, the label is
     * automatically created through the registered {@link org.janusgraph.core.schema.DefaultSchemaMaker}.
     * <p />
     * Attempting to automatically create a vertex label might cause an exception depending on the configuration.
     *
     * @param name
     * @return
     */
    public VertexLabel getOrCreateVertexLabel(String name);


}
