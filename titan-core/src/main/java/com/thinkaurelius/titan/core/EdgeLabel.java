
package com.thinkaurelius.titan.core;

/**
 * EdgeLabel is an extension of {@link RelationType} for edges. Each edge in Titan has a label.
 * <p/>
 * An edge label defines the following characteristics of an edge:
 * <ul>
 * <li><strong>Directionality:</strong> An edge is either directed or unidirected. A directed edge can be thought of
 * as a "normal" edge: it points from one vertex to another and both vertices are aware of the edge's existence. Hence
 * the edge can be traversed in both directions. A unidirected edge is like a hyperlink in that only the out-going
 * vertex is aware of its existence and it can only be traversed in the outgoing direction.</li>
 * <li><strong>Multiplicity:</strong> The multiplicity of an edge imposes restrictions on the number of edges
 * for a particular label that are allowed on a vertex. This allows the definition and enforcement of domain constraints.
 * </li>
 * </ul>
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com)
 * @see RelationType
 */
public interface EdgeLabel extends RelationType {

    /**
     * Checks whether this labels is defined as directed.
     *
     * @return true, if this label is directed, else false.
     */
    public boolean isDirected();

    /**
     * Checks whether this labels is defined as unidirected.
     *
     * @return true, if this label is unidirected, else false.
     */
    public boolean isUnidirected();

    /**
     * The {@link Multiplicity} for this edge label.
     *
     * @return
     */
    public Multiplicity multiplicity();

}
