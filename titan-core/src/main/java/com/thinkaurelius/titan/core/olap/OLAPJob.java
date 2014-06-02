package com.thinkaurelius.titan.core.olap;

import com.thinkaurelius.titan.core.TitanVertex;

/**
 * A vertex centric program which executes on each vertex in the graph.
 * <p/>
 * The program can only inspect the vertex's adjacency list through the pre-computed properties declared in the {@link OLAPJobBuilder} through
 * {@link OLAPQueryBuilder} accessible via {@link com.thinkaurelius.titan.core.olap.OLAPJobBuilder#addQuery()}.
 * In other words, the adjacency list and neighboring vertex's state information is aggregated as declared in {@link OLAPJobBuilder} and
 * then made available as a vertex property retrievable via {@link TitanVertex#getProperty(String)} where the key is equal to the name
 * of the declared query (see {@link OLAPQueryBuilder#setName(String)}. The vertex can also access it's own state which is also
 * a property under the defined key in {@link OLAPJobBuilder#setStateKey(String)}.
 * <p/>
 * Hence, this program can retrieve the vertex's own state and the aggregate state of its neighbors (as previously declared) and then
 * compute the new state for the vertex which is the return argument of the {@link #process(com.thinkaurelius.titan.core.TitanVertex)}
 * method.
 * In other words, the process method retrieves vertex properties to compute a new state for the vertex. Calling any query methods
 * on the vertex itself will yield empty results and modifying the vertex will throw exceptions.

 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPJob<S> {

    public S process(TitanVertex vertex);

}
