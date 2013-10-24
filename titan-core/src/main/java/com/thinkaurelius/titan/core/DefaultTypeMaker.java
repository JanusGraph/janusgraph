package com.thinkaurelius.titan.core;

/**
 * When a graph is configured to automatically create edge labels and property keys when they are first used,
 * a DefaultTypeMaker implementation is used to define them by invoking the {@link #makeLabel(LabelMaker)}
 * or {@link #makeKey(KeyMaker)} methods respectively.
 * <br />
 * By providing a custom DefaultTypeMaker implementation, one can specify how these types should be defined by default.
 * A DefaultTypeMaker implementation is specified in the graph configuration using the full path which means the
 * implementation must be on the classpath. For more information, see the
 * <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see TypeMaker
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
 */
public interface DefaultTypeMaker {

    /**
     * Creates a new label type with default settings against the provided {@link LabelMaker}.
     *
     * @param factory LabelMaker through which the edge label is created
     * @return A new edge label for the given name
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    public TitanLabel makeLabel(LabelMaker factory);

    /**
     * Creates a new property key with default settings against the provided {@link KeyMaker}.
     *
     * @param factory TypeMaker through which the property key is created
     * @return A new property key for the given name
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    public TitanKey makeKey(KeyMaker factory);

    /**
     * Whether to ignore undefined types occurring in a query.
     * <p/>
     * If this method returns true, then undefined types referred to in a {@link TitanVertexQuery} will be silently
     * ignored and an empty result set will be returned. If this method returns false, then usage of undefined types
     * in queries results in an {@link IllegalArgumentException}.
     */
    public boolean ignoreUndefinedQueryTypes();
}
