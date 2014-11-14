package com.thinkaurelius.titan.hadoop.scan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.olap.VertexJobConverter;
import com.thinkaurelius.titan.graphdb.olap.VertexScanJob;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

public class HadoopVertexScanMapper extends HadoopScanMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        /* Don't call super implementation super.setup(context); */
        org.apache.hadoop.conf.Configuration hadoopConf = DEFAULT_COMPAT.getContextConfiguration(context);
        ModifiableHadoopConfiguration scanConf = ModifiableHadoopConfiguration.of(TitanHadoopConfiguration.SCAN_NS, hadoopConf);
        VertexScanJob vertexScan = null; // TODO
        TitanGraph graph = TitanFactory.open(getTitanConfiguration(context));
        job = VertexJobConverter.convert(graph, vertexScan);
        metrics = new HadoopContextScanMetrics(context);
        finishSetup(scanConf);
    }

    private ModifiableConfiguration getTitanConfiguration(Context context) {
        Configuration hadoopConf = DEFAULT_COMPAT.getContextConfiguration(context);
        return ModifiableHadoopConfiguration.of(TitanHadoopConfiguration.SCAN_NS, hadoopConf).getInputConf(GraphDatabaseConfiguration.ROOT_NS);
    }
}
