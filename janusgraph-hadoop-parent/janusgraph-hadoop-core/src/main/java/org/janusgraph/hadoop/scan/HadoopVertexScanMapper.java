package org.janusgraph.hadoop.scan;

import org.janusgraph.core.TitanFactory;
import org.janusgraph.core.TitanGraph;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.olap.VertexJobConverter;
import org.janusgraph.graphdb.olap.VertexScanJob;
import org.janusgraph.hadoop.config.ModifiableHadoopConfiguration;
import org.janusgraph.hadoop.config.TitanHadoopConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import static org.janusgraph.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

public class HadoopVertexScanMapper extends HadoopScanMapper {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        /* Don't call super implementation super.setup(context); */
        org.apache.hadoop.conf.Configuration hadoopConf = DEFAULT_COMPAT.getContextConfiguration(context);
        ModifiableHadoopConfiguration scanConf = ModifiableHadoopConfiguration.of(TitanHadoopConfiguration.MAPRED_NS, hadoopConf);
        VertexScanJob vertexScan = getVertexScanJob(scanConf);
        ModifiableConfiguration graphConf = getTitanConfiguration(context);
        TitanGraph graph = TitanFactory.open(graphConf);
        job = VertexJobConverter.convert(graph, vertexScan);
        metrics = new HadoopContextScanMetrics(context);
        finishSetup(scanConf, graphConf);
    }

    private VertexScanJob getVertexScanJob(ModifiableHadoopConfiguration conf) {
        String jobClass = conf.get(TitanHadoopConfiguration.SCAN_JOB_CLASS);

        try {
            return (VertexScanJob)Class.forName(jobClass).newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
