package com.thinkaurelius.titan.hadoop;

import static com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager.*;
import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.titan.hbase.TitanHBaseInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.titan.cassandra.TitanCassandraInputFormat;

public class TitanIndexRepair {

    private static final Logger log =
            LoggerFactory.getLogger(TitanIndexRepair.class);

    /**
     * Start a reindex job on Cassandra using the supplied TitanGraph configuration
     * on the index specified by the name and type parameters.
     *
     * The config named by {@code titanPropertiesPath} should use one of the Cassandra storage backends.
     *
     * @param titanPropertiesPath a Titan configuration file that could be passed to
     *                           {@link com.thinkaurelius.titan.core.TitanFactory#open(String)}
     * @param indexName the name of the index to repair
     * @param indexType the type of the index to repair
     * @param partitioner the full package.classname of the keyspace partitioner in use
     *                    on the Cassandra cluster, e.g. "org.apache.cassandra.dht.Murmur3Partitioner"
     * @throws Exception
     */
    public static void cassandraRepair(String titanPropertiesPath, String indexName, String indexType, String partitioner) throws Exception {
        Properties p = new Properties();
        p.load(new FileInputStream(titanPropertiesPath));

        cassandraRepair(p, indexName, indexType, partitioner);
    }

    public static void cassandraRepair(Properties titanProperties, String indexName, String indexType, String partitioner) throws Exception {
        Configuration hadoopConfig = new Configuration();
        ConfigHelper.setInputPartitioner(hadoopConfig, partitioner);
        ModifiableHadoopConfiguration titanConf = ModifiableHadoopConfiguration.of(hadoopConfig);
        titanConf.set(TitanHadoopConfiguration.INPUT_FORMAT, TitanCassandraInputFormat.class.getCanonicalName());

        setCommonRepairOptions(titanConf, indexName, indexType);
        copyPropertiesToInputAndOutputConf(hadoopConfig, titanProperties);

        HadoopGraph hg = new HadoopGraph(hadoopConfig);
        repairIndex(hg);

    }

    /**
     * Start a reindex job on HBase using the supplied TitanGraph configuration
     * on the index specified by the name and type parameters.
     *
     * The config named by {@code titanPropertiesPath} should use the HBase storage backend.
     *
     * @param titanPropertiesPath a Titan configuration file that could be passed to
     *                            {@link com.thinkaurelius.titan.core.TitanFactory#open(String)}
     * @param indexName The name of the index to repair
     * @param indexType The type of the index to repair
     * @throws Exception
     */
    public static void hbaseRepair(String titanPropertiesPath, String indexName, String indexType) throws Exception {
        Properties p = new Properties();
        p.load(new FileInputStream(titanPropertiesPath));
        hbaseRepair(p, indexName, indexType);
    }

    public static void hbaseRepair(Properties titanProperties, String indexName, String indexType) throws Exception {
        Configuration hadoopConfig = new Configuration();
        ModifiableHadoopConfiguration titanConf = ModifiableHadoopConfiguration.of(hadoopConfig);
        titanConf.set(TitanHadoopConfiguration.INPUT_FORMAT, TitanHBaseInputFormat.class.getCanonicalName());

        setCommonRepairOptions(titanConf, indexName, indexType);
        copyPropertiesToInputAndOutputConf(hadoopConfig, titanProperties);

        HadoopGraph hg = new HadoopGraph(hadoopConfig);
        repairIndex(hg);
    }

    public static void main(String args[]) throws Exception {
        HadoopGraph hg = HadoopFactory.open(args[0]);
        repairIndex(hg);
    }

    public static void repairIndex(HadoopGraph hg) throws Exception {
        HadoopCompiler compiler = DEFAULT_COMPAT.newCompiler(hg);
        Class<? extends Mapper<?,?,?,?>> mapper = TitanIndexRepairMapper.class;
        compiler.addMap(mapper, NullWritable.class, NullWritable.class, hg.getConf());
        compiler.completeSequence();
        compiler.run(new String[]{});
        hg.shutdown();
    }

    private static void setCommonRepairOptions(ModifiableHadoopConfiguration titanConf, String indexName, String indexType) {
        titanConf.set(TitanHadoopConfiguration.INDEX_NAME, indexName);
        titanConf.set(TitanHadoopConfiguration.INDEX_TYPE, indexType);
        log.info("Set input format {}", titanConf.get(TitanHadoopConfiguration.INPUT_FORMAT));
        titanConf.set(TitanHadoopConfiguration.OUTPUT_FORMAT, NullOutputFormat.class.getCanonicalName());
        log.info("Set output format {}", titanConf.get(TitanHadoopConfiguration.OUTPUT_FORMAT));
        titanConf.set(TitanHadoopConfiguration.SIDE_EFFECT_FORMAT, TextOutputFormat.class.getCanonicalName());
        titanConf.set(TitanHadoopConfiguration.JOBDIR_LOCATION, "jobs");
        titanConf.set(TitanHadoopConfiguration.JOBDIR_OVERWRITE, true);
    }

    private static void copyPropertiesToInputAndOutputConf(Configuration sink, Properties source) {
        for (Map.Entry<Object, Object> e : source.entrySet()) {
            String k;
            String v = e.getValue().toString();
            k = ConfigElement.getPath(TitanHadoopConfiguration.INPUT_CONF_NS) + "." + e.getKey().toString();
            sink.set(k, v);
            log.info("Set {}={}", k, v);
            k = ConfigElement.getPath(TitanHadoopConfiguration.OUTPUT_CONF_NS) + "." + e.getKey().toString();
            sink.set(k, v);
            log.info("Set {}={}", k, v);
        }
    }
}
