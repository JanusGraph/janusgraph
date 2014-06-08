package com.thinkaurelius.titan.core.schema;

/**
 * Used to read and change the global Titan configuration.
 * The configuration options are read from the graph and affect the entire database.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanConfiguration {

    /**
     * Returns a string representation of the provided configuration option or namespace for inspection.
     * <p />
     * An exception is thrown if the path is invalid.
     *
     * @param path
     * @return
     */
    public String get(String path);

    /**
     * Sets the configuration option identified by the provided path to the given value.
     *
     * @param path
     * @param value
     */
    public TitanConfiguration set(String path, Object value);

}
