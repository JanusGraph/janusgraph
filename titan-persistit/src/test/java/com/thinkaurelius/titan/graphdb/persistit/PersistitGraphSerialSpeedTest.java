package com.thinkaurelius.titan.graphdb.persistit;

import com.thinkaurelius.titan.PersistitStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.graphdb.TitanGraphSerialSpeedTest;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.testutil.gen.Schema;

public class PersistitGraphSerialSpeedTest extends TitanGraphSerialSpeedTest {
    
    private static StandardTitanGraph graph; 
    private static Schema schema;
    
    public PersistitGraphSerialSpeedTest() throws StorageException {
        super(PersistitStorageSetup.getPersistitGraphConfig());
    }

    @Override
    protected StandardTitanGraph getGraph() throws StorageException {
        if (null == graph) {
            graph = (StandardTitanGraph)TitanFactory.open(conf);
            initializeGraph(graph);
        }
        return graph;
    }
    
    @Override
    protected Schema getSchema() {
        if (null == schema) {
            schema = new Schema.Builder(VERTEX_COUNT, EDGE_COUNT).build();
        }
        return schema;
    }
}
