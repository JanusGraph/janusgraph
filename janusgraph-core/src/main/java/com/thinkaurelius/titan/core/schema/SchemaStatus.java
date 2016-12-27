package com.thinkaurelius.titan.core.schema;

/**
 * Designates the status of a {@link TitanIndex} in a graph.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum SchemaStatus {

    /**
     * The index is installed in the system but not yet registered with all instances in the cluster
     */
    INSTALLED,

    /**
     * The index is registered with all instances in the cluster but not (yet) enabled
     */
    REGISTERED,

    /**
     * The index is enabled and in use
     */
    ENABLED,

    /**
     * The index is disabled and no longer in use
     */
    DISABLED;


    public boolean isStable() {
        switch(this) {
            case INSTALLED: return false;
            default: return true;
        }
    }

}
