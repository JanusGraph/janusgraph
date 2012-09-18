package com.thinkaurelius.faunus.formats.titan.hbase;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class TitanHBaseInputFormat extends InputFormat<NullWritable, FaunusVertex> {

    private static final Logger logger = LoggerFactory.getLogger(TitanHBaseInputFormat.class);

    private static final String HOSTNAME_KEY = HBaseStorageManager.HBASE_CONFIGURATION_MAP.get(StorageManager.HOSTNAME_KEY);
    private static final String PORT_KEY = HBaseStorageManager.HBASE_CONFIGURATION_MAP.get(StorageManager.PORT_KEY);


    static final byte[] EDGE_STORE_FAMILY = Bytes.toBytes(GraphDatabaseConfiguration.STORAGE_EDGESTORE_NAME);

    /**
     * Holds the details for the internal scanner.
     */
    private Scan scan = null;
    /**
     * The table to scan.
     */
    private HTable table = null;
    /**
     * The graph interfacing with the same Hbase backend
     */
    private FaunusTitanHBaseGraph graph = null;
    private boolean pathEnabled = false;


    /**
     * Builds a TableRecordReader. If no TableRecordReader was provided, uses
     * the default.
     *
     * @param split   The split to work with.
     * @param context The current context.
     * @return The newly created record reader.
     * @throws IOException When creating the reader fails.
     * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(
     *org.apache.hadoop.mapreduce.InputSplit,
     *      org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public RecordReader<NullWritable, FaunusVertex> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        if (table == null) {
            initialize(context.getConfiguration());
        }

        TableSplit tSplit = (TableSplit) split;
        Scan sc = new Scan(this.scan);
        sc.setStartRow(tSplit.getStartRow());
        sc.setStopRow(tSplit.getEndRow());
        TitanHBaseRecordReader rr = new TitanHBaseRecordReader(table, scan, graph, pathEnabled);
        rr.init();
        return rr;
    }

    private void initialize(final Configuration config) throws IOException {
        String tablename = config.get("hbase.input.table");
        table = new HTable(HBaseConfiguration.create(config), tablename);

        scan = new Scan();
        scan.addFamily(EDGE_STORE_FAMILY);

        //  ## Instantiate Titan ##
        final BaseConfiguration titanconfig = new BaseConfiguration();
        //General Titan configuration for read-only
        titanconfig.setProperty("storage.read-only", "true");
        titanconfig.setProperty("autotype", "none");
        //Cassandra specific configuration
        titanconfig.setProperty("storage.backend", "hbase");
        titanconfig.setProperty("storage.tablename", tablename);
        titanconfig.setProperty("storage.hostname", config.get(HOSTNAME_KEY));
        if (config.get(PORT_KEY, null) != null)
            titanconfig.setProperty("storage.port", config.get(PORT_KEY));
        graph = new FaunusTitanHBaseGraph(titanconfig);

        pathEnabled = config.getBoolean(FaunusCompiler.PATH_ENABLED, false);
    }

    /**
     * Calculates the splits that will serve as input for the map tasks. The
     * number of splits matches the number of regions in a table.
     *
     * @param context The current job context.
     * @return The list of input splits.
     * @throws IOException When creating the list of splits fails.
     * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
     *org.apache.hadoop.mapreduce.JobContext)
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        final Configuration conf = context.getConfiguration();
        initialize(conf);

        if (table == null) throw new IOException("No table was provided.");

        Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
        if (keys == null || keys.getFirst() == null ||
                keys.getFirst().length == 0) {
            throw new IOException("Expecting at least one region.");
        }
        int count = 0;
        List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length);
        for (int i = 0; i < keys.getFirst().length; i++) {
//            if ( !includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
//                continue;
//            }
            String regionLocation = table.getRegionLocation(keys.getFirst()[i]).
                    getServerAddress().getHostname();
            byte[] startRow = scan.getStartRow();
            byte[] stopRow = scan.getStopRow();
            // determine if the given start an stop key fall into the region
            if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
                    Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
                    (stopRow.length == 0 ||
                            Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
                byte[] splitStart = startRow.length == 0 ||
                        Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
                        keys.getFirst()[i] : startRow;
                byte[] splitStop = (stopRow.length == 0 ||
                        Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
                        keys.getSecond()[i].length > 0 ?
                        keys.getSecond()[i] : stopRow;
                InputSplit split = new TableSplit(table.getTableName(),
                        splitStart, splitStop, regionLocation);
                splits.add(split);
                count++;
                logger.debug("getSplits: split -> {} -> {}", count, split);
            }
        }
        return splits;
    }


}