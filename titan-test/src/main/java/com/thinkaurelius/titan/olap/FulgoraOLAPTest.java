package com.thinkaurelius.titan.olap;

import com.thinkaurelius.titan.core.olap.OLAPJobBuilder;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.fulgora.FulgoraBuilder;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class FulgoraOLAPTest extends OLAPTest {

    protected <S> OLAPJobBuilder<S> getOLAPBuilder(StandardTitanGraph graph, Class<S> clazz) {
        return new FulgoraBuilder<S>(graph);
    }

}
