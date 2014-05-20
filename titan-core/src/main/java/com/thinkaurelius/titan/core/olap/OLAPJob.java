package com.thinkaurelius.titan.core.olap;

import com.thinkaurelius.titan.core.TitanVertex;

/**
 * A vertex centric program which executes on each vertex in the graph. The program may only access the incident
 * edges and properties on the given (i.e. passed-in) vertex. It cannot access neighboring vertices beyond their id and vertex state.
 * The initial vertex state is defined in the {@link OLAPJobBuilder} and is mutated during the execution of a vertex program.
 * While the neighboring vertex state can be accessed, only this vertex's vertex state can be modified.
 * </p>
 * Vertices cannot be modified beyond setting the vertex state of the provided vertex. In other words, the graph is
 * immutable aside from the vertex state.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPJob {

    public void process(TitanVertex vertex);

}
