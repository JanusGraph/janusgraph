package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.VertexLabel;

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
     * Checks whether a property key of the given name has been defined in the Titan schema.
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
     * Checks whether an edge label of the given name has been defined in the Titan schema.
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
     * Returns the vertex label with the given name. If a vertex label with this name does not exist, the label is
     * automatically created through the registered {@link com.thinkaurelius.titan.core.schema.DefaultSchemaMaker}.
     * <p />
     * Attempting to automatically create a vertex label might cause an exception depending on the configuration.
     *
     * @param name
     * @return
     */
    public VertexLabel getVertexLabel(String name);


}
