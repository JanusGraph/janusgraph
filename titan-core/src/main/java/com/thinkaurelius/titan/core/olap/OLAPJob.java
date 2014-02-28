package com.thinkaurelius.titan.core.olap;

import com.thinkaurelius.titan.core.TitanVertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OLAPJob {

    public void process(TitanVertex vertex);

}
