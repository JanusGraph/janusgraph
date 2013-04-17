package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.conf.Configuration;

/**
 * A filtering mechanism for, if possible, only getting particular aspects of a vertex from the graph.
 * A minimum, the filtering is done on the Hadoop side of the computation.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (marko@thinkaurelius.com)
 */

public class InputGraphFilter {

    public static final String FAUNUS_GRAPH_INPUT_FILTER = "faunus.graph.input.filter";

    private boolean filterEdges;

    public InputGraphFilter() {
        this.filterEdges = false;
    }

    public InputGraphFilter(final Configuration configuration) {
        this.filterEdges = configuration.getBoolean(FAUNUS_GRAPH_INPUT_FILTER, false);
    }

    public void setFilterEdges(final boolean filterEdges) {
        this.filterEdges = filterEdges;
    }

    public boolean hasFilterEdges() {
        return this.filterEdges;
    }

    public void setConfiguration(final Configuration configuration) {
        configuration.setBoolean(FAUNUS_GRAPH_INPUT_FILTER, this.filterEdges);
    }

    public void defaultVertexFilter(final FaunusVertex vertex) {
        if(filterEdges)
            vertex.removeEdges(Tokens.Action.DROP, Direction.BOTH);
    }
}
