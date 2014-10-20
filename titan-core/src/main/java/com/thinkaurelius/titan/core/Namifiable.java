package com.thinkaurelius.titan.core;

/**
 * Represents an entity that can be uniquely identified by a String name.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Namifiable {

    /**
     * Returns the unique name of this entity.
     *
     * @return Name of this entity.
     */
    public String name();

}
