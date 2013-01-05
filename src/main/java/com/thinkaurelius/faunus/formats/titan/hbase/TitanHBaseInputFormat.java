package com.thinkaurelius.faunus.formats.titan.hbase;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.titan.GraphFactory;
import com.thinkaurelius.faunus.formats.titan.TitanInputFormat;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.titan.diskstorage.Backend;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TitanHBaseInputFormat extends TitanInputFormat {

    public static final String TITAN_GRAPH_INPUT_STORAGE_TABLENAME = "titan.graph.input.storage.tablename";
    static final byte[] EDGE_STORE_FAMILY = Bytes.toBytes(Backend.EDGESTORE_NAME);

    private final TableInputFormat tableInputFormat;
    private FaunusTitanHBaseGraph graph;
    private boolean pathEnabled;


    public TitanHBaseInputFormat() {
        this.tableInputFormat = new TableInputFormat();
    }

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.tableInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TitanHBaseRecordReader(this.graph, this.pathEnabled, (TableRecordReader) this.tableInputFormat.createRecordReader(inputSplit, taskAttemptContext));
    }

    @Override
    public void setConf(final Configuration config) {
        config.set(TableInputFormat.SCAN_COLUMN_FAMILY, Backend.EDGESTORE_NAME);
        config.set(TableInputFormat.INPUT_TABLE, config.get(TITAN_GRAPH_INPUT_STORAGE_TABLENAME));
        config.set(HConstants.ZOOKEEPER_QUORUM, config.get(TITAN_GRAPH_INPUT_STORAGE_HOSTNAME));
        if (config.get(TITAN_GRAPH_INPUT_STORAGE_PORT, null) != null)
            config.set(HConstants.ZOOKEEPER_CLIENT_PORT, config.get(TITAN_GRAPH_INPUT_STORAGE_PORT));
        config.set("storage.read-only","true");
        config.set("autotype","none");
        this.tableInputFormat.setConf(config);
        this.graph = new FaunusTitanHBaseGraph(GraphFactory.generateTitanConfiguration(config, TITAN_GRAPH_INPUT));
        this.pathEnabled = config.getBoolean(FaunusCompiler.PATH_ENABLED, false);
    }

    @Override
    public Configuration getConf() {
        return tableInputFormat.getConf();
    }


}
