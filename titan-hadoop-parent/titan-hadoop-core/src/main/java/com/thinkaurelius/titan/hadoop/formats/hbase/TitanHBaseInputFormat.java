package com.thinkaurelius.titan.hadoop.formats.hbase;

import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.formats.util.TitanInputFormat;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseInputFormat extends TitanInputFormat {

    private final TableInputFormat tableInputFormat = new TableInputFormat();
    private TitanHBaseHadoopGraph graph;
    private byte[] edgestoreFamily;

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.tableInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TitanHBaseRecordReader(this.graph, this.vertexQuery, (TableRecordReader) this.tableInputFormat.createRecordReader(inputSplit, taskAttemptContext), edgestoreFamily);
    }

    @Override
    public void setConf(final Configuration config) {
        super.setConf(config);
        this.graph = new TitanHBaseHadoopGraph(titanSetup);

        //config.set(TableInputFormat.SCAN_COLUMN_FAMILY, Backend.EDGESTORE_NAME);
        config.set(TableInputFormat.INPUT_TABLE, inputConf.get(HBaseStoreManager.HBASE_TABLE));
        //config.set(HConstants.ZOOKEEPER_QUORUM, config.get(TITAN_HADOOP_GRAPH_INPUT_TITAN_STORAGE_HOSTNAME));
        config.set(HConstants.ZOOKEEPER_QUORUM, inputConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
//        if (basicConf.get(TITAN_HADOOP_GRAPH_INPUT_TITAN_STORAGE_PORT, null) != null)
        if (inputConf.has(GraphDatabaseConfiguration.STORAGE_PORT))
            config.set(HConstants.ZOOKEEPER_CLIENT_PORT, String.valueOf(inputConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        // TODO: config.set("storage.read-only", "true");
        config.set("autotype", "none");
        Scan scanner = new Scan();
        // TODO the mapping is private in HBaseStoreManager and leaks here -- replace String database/CF names with an enum where each value has both a short and long name
        if (inputConf.get(HBaseStoreManager.SHORT_CF_NAMES)) {
            scanner.addFamily("e".getBytes());
            edgestoreFamily = Bytes.toBytes("e");
        } else {
            scanner.addFamily(Backend.EDGESTORE_NAME.getBytes());
            edgestoreFamily = Bytes.toBytes(Backend.EDGESTORE_NAME);
        }
        scanner.setFilter(getColumnFilter(titanSetup.inputSlice(this.vertexQuery)));
        //TODO (minor): should we set other options in http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Scan.html for optimization?
        Method converter;
        try {
            converter = TableMapReduceUtil.class.getDeclaredMethod("convertScanToString", Scan.class);
            converter.setAccessible(true);
            config.set(TableInputFormat.SCAN, (String) converter.invoke(null, scanner));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.tableInputFormat.setConf(config);
    }

    private Filter getColumnFilter(SliceQuery query) {
        return null;
        //TODO: return HBaseKeyColumnValueStore.getFilter(titanSetup.inputSlice(inputFilter));
    }

    @Override
    public Configuration getConf() {
        return tableInputFormat.getConf();
    }
}
