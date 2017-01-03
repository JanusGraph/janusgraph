package org.janusgraph.core;

/**
 * Exception thrown due to invalid configuration options or when errors
 * occur during the configuration and initialization of Janus.
 * <p/>
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class JanusConfigurationException extends JanusException {

    private static final long serialVersionUID = 4056436257763972423L;

    /**
     * @param msg Exception message
     */
    public JanusConfigurationException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public JanusConfigurationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public JanusConfigurationException(Throwable cause) {
        this("Exception in graph database configuration", cause);
    }

}
