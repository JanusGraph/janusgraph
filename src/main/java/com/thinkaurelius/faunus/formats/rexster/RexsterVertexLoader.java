package com.thinkaurelius.faunus.formats.rexster;

import org.codehaus.jettison.json.JSONArray;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface RexsterVertexLoader {
    public JSONArray getPagedVertices(final long start, final long end);
}
