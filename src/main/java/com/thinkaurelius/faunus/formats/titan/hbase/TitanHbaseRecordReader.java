package com.thinkaurelius.faunus.formats.titan.hbase;

import com.thinkaurelius.faunus.FaunusVertex;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NavigableMap;

/**
 * Iterate over an HBase table data, return (ImmutableBytesWritable, Result)
 * pairs.
 */
public class TitanHBaseRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private static final Logger logger = LoggerFactory.getLogger(TitanHBaseRecordReader.class);


    private Scan scan = null;
    private HTable htable = null;
    private ResultScanner scanner = null;

    private final FaunusTitanHBaseGraph graph;
    private boolean pathEnabled;

    private byte[] lastRow = null;
    private FaunusVertex currentVertex = null;


    public TitanHBaseRecordReader(final HTable table, final Scan scan, FaunusTitanHBaseGraph graph, boolean pathEnabled) {
        this.htable = table;
        this.scan = scan;
        this.graph = graph;
        this.pathEnabled = pathEnabled;
    }

    /**
     * Restart from survivable exceptions by creating a new scanner.
     *
     * @param firstRow The first row to start at.
     * @throws IOException When restarting fails.
     */
    public void restart(byte[] firstRow) throws IOException {
        Scan newScan = new Scan(scan);
        newScan.setStartRow(firstRow);
        this.scanner = this.htable.getScanner(newScan);
    }

    /**
     * Build the scanner. Not done in constructor to allow for extension.
     *
     * @throws IOException When restarting the scan fails.
     */
    public void init() throws IOException {
        restart(scan.getStartRow());
    }

    /**
     * Closes the split.
     */
    public void close() {
        this.scanner.close();
    }

    /**
     * Returns the current key.
     *
     * @return The current key.
     * @throws IOException
     * @throws InterruptedException When the job is aborted.
     */
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    /**
     * Returns the current value.
     *
     * @return The current value.
     * @throws IOException          When the value is faulty.
     * @throws InterruptedException When the job is aborted.
     */
    public FaunusVertex getCurrentValue() throws IOException, InterruptedException {
        return currentVertex;
    }


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Positions the record reader to the next record.
     *
     * @return <code>true</code> if there was another record.
     * @throws IOException          When reading the record failed.
     * @throws InterruptedException When the job was aborted.
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
        Result result = null;
        while (true) {
            try {
                result = this.scanner.next();
            } catch (IOException e) {
                logger.debug("recovered from " + StringUtils.stringifyException(e));
                if (lastRow == null) {
                    logger.warn("We are restarting the first next() invocation," +
                            " if your mapper's restarted a few other times like this" +
                            " then you should consider killing this job and investigate" +
                            " why it's taking so long.");
                    lastRow = scan.getStartRow();
                }
                restart(lastRow);
                scanner.next();    // skip presumed already mapped row
                result = scanner.next();
            }
            if (result != null && result.size() > 0) {
                lastRow = result.getRow();
                //Is this a valid row?
                final NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap = result.getMap().get(TitanHBaseInputFormat.EDGE_STORE_FAMILY);
                if (rowMap != null) {
                    //Parse FaunusVertex
                    currentVertex = graph.readFaunusVertex(lastRow, rowMap);
                    if (null != currentVertex) {
                        if (this.pathEnabled) currentVertex.enablePath(true);
                        return true;
                    }
                }
            } else {
                return false;
            }
        }
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return A number between 0.0 and 1.0, the fraction of the data read.
     */
    public float getProgress() {
        // Depends on the total number of tuples
        return 0;
    }

}