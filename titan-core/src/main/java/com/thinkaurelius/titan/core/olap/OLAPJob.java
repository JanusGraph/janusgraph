package com.thinkaurelius.titan.core.olap;

import com.thinkaurelius.titan.core.TitanVertex;

/**
 * A vertex centric program which executes on each vertex in the graph. The program may only access the incident
 * edges and properties on the given vertex. It cannot access neighboring vertices beyond their id and vertex state.
 * </p>
 * Vertices cannot be modified beyond setting the vertex state of the provided vertex.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPJob {

    public void process(TitanVertex vertex);

}
