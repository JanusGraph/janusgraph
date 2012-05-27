package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.graphdb.transaction.InMemoryTitanGraph;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfig;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TitanInMemoryBlueprintsGraph extends InMemoryTitanGraph implements Graph, KeyIndexableGraph {

    public TitanInMemoryBlueprintsGraph() {
        super(new TransactionConfig(BlueprintsDefaultTypeMaker.INSTANCE,true));
    }

    @Override
    public Features getFeatures() {
        Features features = TitanFeatures.getBaselineTitanFeatures();
        features.isPersistent = false;
        return features;
    }

}
