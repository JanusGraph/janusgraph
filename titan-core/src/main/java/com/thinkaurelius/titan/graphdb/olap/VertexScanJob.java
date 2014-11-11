package com.thinkaurelius.titan.graphdb.olap;

import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexScanJob {

    public default void setup(Configuration config, ScanMetrics metrics) {}

    public default void teardown(ScanMetrics metrics) {}

    public void process(TitanVertex vertex, ScanMetrics metrics);

    public void getQueries(QueryContainer queries);


}
