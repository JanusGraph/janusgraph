package org.janusgraph.core.schema;

/**
 * Used to read and change the global Janus configuration.
 * The configuration options are read from the graph and affect the entire database.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusConfiguration {

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
    public JanusConfiguration set(String path, Object value);

}
