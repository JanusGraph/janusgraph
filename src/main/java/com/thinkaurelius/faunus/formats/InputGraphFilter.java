package com.thinkaurelius.faunus.formats;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class InputGraphFilter {

    private boolean filterEdges;


    public InputGraphFilter() {
        filterEdges = false;
    }

    public void setFilterEdges(final boolean filterEdges) {
        this.filterEdges=filterEdges;
    }

    public boolean hasFilterEdges() {
        return this.filterEdges;
    }
}
