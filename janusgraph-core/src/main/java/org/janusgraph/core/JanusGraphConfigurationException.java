package org.janusgraph.core;

/**
 * Exception thrown due to invalid configuration options or when errors
 * occur during the configuration and initialization of JanusGraph.
 * <p/>
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class JanusGraphConfigurationException extends JanusGraphException {

    private static final long serialVersionUID = 4056436257763972423L;

    /**
     * @param msg Exception message
     */
    public JanusGraphConfigurationException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public JanusGraphConfigurationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public JanusGraphConfigurationException(Throwable cause) {
        this("Exception in graph database configuration", cause);
    }

}
