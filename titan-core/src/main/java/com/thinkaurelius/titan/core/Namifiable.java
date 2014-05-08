package com.thinkaurelius.titan.core;

/**
 * Represents an entity that can be uniquely identified by a String name.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Namifiable {

    /**
     * Returns the unique name of this type.
     *
     * @return Name of this type.
     */
    public String getName();

}
