package com.thinkaurelius.titan.core;

/**
 * When a graph is configured to automatically create edge labels and property keys when they are first used,
 * a DefaultTypeMaker implementation is used to define them by invoking the {@link #makeLabel(String, TypeMaker)}
 * or {@link #makeKey(String, TypeMaker)} methods respectively.
 * <br />
 * By providing a custom DefaultTypeMaker implementation, one can specify how these types should be defined by default.
 * A DefaultTypeMaker implementation is specified in the graph configuration using the full path which means the
 * implementation must be on the classpath. For more information, see the
 * <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
 *
 * @see TypeMaker
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
 *
 * @author	Matthias Br&ouml;cheler (http://www.matthiasb.com)
*/
public interface DefaultTypeMaker {

    /**
     * Creates a new label type with the given name and default settings against the provided TypeMaker.
     *
     * @param name Name of the label
     * @param factory TypeMaker through which the edge label is created
     * @return A new edge label for the given name
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    public TitanLabel makeLabel(String name, TypeMaker factory);

    /**
     * Creates a new property key with the given name and default settings against the provided TypeMaker.
     *
     * @param name Name of the property key
     * @param factory TypeMaker through which the property key is created
     * @return A new property key for the given name
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    public TitanKey makeKey(String name, TypeMaker factory);
}
