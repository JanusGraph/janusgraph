package com.thinkaurelius.titan.core;

/**
 * Represents an entity that can be uniquely identified by a long id.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Idfiable {

    /**
     * Unique identifier for this entity.
     *
     * @return Unique long id for this entity
     */
    public long longId();

}
