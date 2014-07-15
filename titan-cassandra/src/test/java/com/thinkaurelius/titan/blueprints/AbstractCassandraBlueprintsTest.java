package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class AbstractCassandraBlueprintsTest extends TitanBlueprintsTest {

    @Override
    public void beforeSuite() {
        //Do nothing
    }

    @Override
    public TitanGraph openGraph(String uid) {
        return TitanFactory.open(getGraphConfig());
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    protected abstract WriteConfiguration getGraphConfig();
}
