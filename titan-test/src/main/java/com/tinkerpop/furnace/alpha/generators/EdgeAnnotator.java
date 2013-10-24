package com.tinkerpop.furnace.alpha.generators;

import com.tinkerpop.blueprints.Edge;

/**
 * EdgeAnnotator is used to assign properties to generated edges.
 * 
 * During synthetic network generation, {@link #annotate(com.tinkerpop.blueprints.Edge)} is
 * called on each newly generated edge exactly once. Hence, an implementation of this
 * interface can assign arbitrary properties to this edge.
 * 
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface EdgeAnnotator {

    /**
     * Empty {@link EdgeAnnotator}. Does not assign any properties
     */
    public final EdgeAnnotator NONE = new EdgeAnnotator() {
        @Override
        public void annotate(Edge edge) {
            //Do nothing
        }
    };

    /**
     * An implementation of this method can assign properties to the edge.
     * This method is called once for each newly generated edge.
     *
     * @param edge Newly generated edge
     */
    public void annotate(Edge edge);

}
