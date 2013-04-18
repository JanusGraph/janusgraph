package com.thinkaurelius.faunus.formats.titan.hbase;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import com.thinkaurelius.faunus.formats.titan.GraphFactory;
import com.thinkaurelius.faunus.formats.titan.TitanInputFormat;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
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

    public static final String FAUNUS_GRAPH_INPUT_TITAN_STORAGE_TABLENAME = "faunus.graph.input.titan.storage.tablename";
    static final byte[] EDGE_STORE_FAMILY = Bytes.toBytes(Backend.EDGESTORE_NAME);

    private final TableInputFormat tableInputFormat = new TableInputFormat();
    private FaunusTitanHBaseGraph graph;
    private VertexQueryFilter vertexQuery;
    private boolean pathEnabled;

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.tableInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new TitanHBaseRecordReader(this.graph, this.vertexQuery, this.pathEnabled, (TableRecordReader) this.tableInputFormat.createRecordReader(inputSplit, taskAttemptContext));
    }

    @Override
    public void setConf(final Configuration config) {
        this.graph = new FaunusTitanHBaseGraph(GraphFactory.generateTitanConfiguration(config, FAUNUS_GRAPH_INPUT_TITAN));
        this.vertexQuery = VertexQueryFilter.create(config);
        this.pathEnabled = config.getBoolean(FaunusCompiler.PATH_ENABLED, false);

        //config.set(TableInputFormat.SCAN_COLUMN_FAMILY, Backend.EDGESTORE_NAME);
        config.set(TableInputFormat.INPUT_TABLE, config.get(FAUNUS_GRAPH_INPUT_TITAN_STORAGE_TABLENAME));
        config.set(HConstants.ZOOKEEPER_QUORUM, config.get(FAUNUS_GRAPH_INPUT_TITAN_STORAGE_HOSTNAME));
        if (config.get(FAUNUS_GRAPH_INPUT_TITAN_STORAGE_PORT, null) != null)
            config.set(HConstants.ZOOKEEPER_CLIENT_PORT, config.get(FAUNUS_GRAPH_INPUT_TITAN_STORAGE_PORT));
        config.set("storage.read-only", "true");
        config.set("autotype", "none");
        Scan scanner = new Scan();
        scanner.addFamily(Backend.EDGESTORE_NAME.getBytes());
        scanner.setFilter(getColumnFilter(this.vertexQuery));
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

    private Filter getColumnFilter(VertexQueryFilter inputFilter) {
        return getFilter(TitanInputFormat.inputSlice(inputFilter, graph));
    }

    //TODO: replace by HBaseKeyColumnValueStore.getFilter(SliceQuery) when Titan 0.3.1 is released!
    public static Filter getFilter(SliceQuery query) {
        byte[] colStartBytes = query.getSliceEnd().hasRemaining() ? ByteBufferUtil.getArray(query.getSliceStart()) : null;
        byte[] colEndBytes = query.getSliceEnd().hasRemaining() ? ByteBufferUtil.getArray(query.getSliceEnd()) : null;

        Filter filter = new ColumnRangeFilter(colStartBytes, true, colEndBytes, false);

        if (query.hasLimit()) {
            filter = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                    filter,
                    new ColumnPaginationFilter(query.getLimit(), 0));
        }

        return filter;
    }

    @Override
    public Configuration getConf() {
        return tableInputFormat.getConf();
    }
}
