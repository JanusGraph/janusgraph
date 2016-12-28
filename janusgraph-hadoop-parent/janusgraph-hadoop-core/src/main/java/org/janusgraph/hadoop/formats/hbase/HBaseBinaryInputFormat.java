package com.thinkaurelius.titan.hadoop.formats.hbase;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.util.AbstractBinaryInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

public class HBaseBinaryInputFormat extends AbstractBinaryInputFormat {

    private static final Logger log = LoggerFactory.getLogger(HBaseBinaryInputFormat.class);

    private final TableInputFormat tableInputFormat = new TableInputFormat();
    private TableRecordReader tableReader;
    private byte[] inputCFBytes;
    private RecordReader<StaticBuffer, Iterable<Entry>> titanRecordReader;

    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException {
        return this.tableInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<StaticBuffer, Iterable<Entry>> createRecordReader(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        tableReader =
                (TableRecordReader) tableInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        titanRecordReader =
                new HBaseBinaryRecordReader(tableReader, inputCFBytes);
        return titanRecordReader;
    }

    @Override
    public void setConf(final Configuration config) {
        super.setConf(config);

        //config.set(TableInputFormat.SCAN_COLUMN_FAMILY, Backend.EDGESTORE_NAME);
        config.set(TableInputFormat.INPUT_TABLE, titanConf.get(HBaseStoreManager.HBASE_TABLE));
        //config.set(HConstants.ZOOKEEPER_QUORUM, config.get(TITAN_HADOOP_GRAPH_INPUT_TITAN_STORAGE_HOSTNAME));
        config.set(HConstants.ZOOKEEPER_QUORUM, titanConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
//        if (basicConf.get(TITAN_HADOOP_GRAPH_INPUT_TITAN_STORAGE_PORT, null) != null)
        if (titanConf.has(GraphDatabaseConfiguration.STORAGE_PORT))
            config.set(HConstants.ZOOKEEPER_CLIENT_PORT, String.valueOf(titanConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        config.set("autotype", "none");
        log.debug("hbase.security.authentication={}", config.get("hbase.security.authentication"));
        Scan scanner = new Scan();
        String cfName = mrConf.get(TitanHadoopConfiguration.COLUMN_FAMILY_NAME);
        // TODO the space-saving short name mapping leaks from HBaseStoreManager here
        if (titanConf.get(HBaseStoreManager.SHORT_CF_NAMES)) {
            try {
                cfName = HBaseStoreManager.shortenCfName(cfName);
            } catch (PermanentBackendException e) {
                throw new RuntimeException(e);
            }
        }
        scanner.addFamily(cfName.getBytes());
        inputCFBytes = Bytes.toBytes(cfName);

        //scanner.setFilter(getColumnFilter(titanSetup.inputSlice(this.vertexQuery))); // TODO
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

    public TableRecordReader getTableReader() {
        return tableReader;
    }

    public byte[] getEdgeStoreFamily() {
        return inputCFBytes;
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
