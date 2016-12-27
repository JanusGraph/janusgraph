package com.thinkaurelius.titan.core.schema;

/**
 * Used to control Titan's consistency behavior on eventually consistent or other non-transactional backend systems.
 * The consistency behavior can be defined for individual {@link TitanSchemaElement}s which then applies to all instances.
 * <p/>
 * Consistency modifiers are installed on schema elements via {@link TitanManagement#setConsistency(TitanSchemaElement, ConsistencyModifier)}
 * and can be read using {@link TitanManagement#getConsistency(TitanSchemaElement)}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum ConsistencyModifier {

    /**
     * Uses the default consistency model guaranteed by the enclosing transaction against the configured
     * storage backend.
     * </p>
     * What this means exactly, depends on the configuration of the storage backend as well as the (optional) configuration
     * of the enclosing transaction.
     */
    DEFAULT,

    /**
     * Locks will be explicitly acquired to guarantee consistency if the storage backend supports locks.
     * </p>
     * The exact consistency guarantees depend on the configured lock implementation.
     * </p>
     * Note, that locking may be ignored under certain transaction configurations.
     */
    LOCK,


    /**
     * Causes Titan to delete and add a new edge/property instead of overwriting an existing one, hence avoiding potential
     * concurrent write conflicts. This only applies to multi-edges and list-properties.
     * </p>
     * Note, that this potentially impacts how the data should be read.
     */
    FORK;

}
