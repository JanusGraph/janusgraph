package com.thinkaurelius.titan.hadoop;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
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

    public static void cassandraRepair(String titanPropertiesPath, String indexName, String indexType) throws Exception {
        cassandraRepair(titanPropertiesPath, indexName, indexType, "org.apache.cassandra.dht.Murmur3Partitioner");
    }

    public static void cassandraRepair(String titanPropertiesPath, String indexName, String indexType, String partitioner) throws Exception {
        Properties p = new Properties();
        p.load(new FileInputStream(titanPropertiesPath));

        cassandraRepair(p, indexName, indexType, partitioner);
    }

    public static void cassandraRepair(Properties titanProperties, String indexName, String indexType, String partitioner) throws Exception {
        Configuration hadoopConfig = new Configuration();
        ModifiableHadoopConfiguration titanConf = ModifiableHadoopConfiguration.of(hadoopConfig);

        titanConf.set(TitanHadoopConfiguration.INDEX_NAME, indexName);
        titanConf.set(TitanHadoopConfiguration.INDEX_TYPE, indexType);
        titanConf.set(TitanHadoopConfiguration.INPUT_FORMAT, TitanCassandraInputFormat.class.getCanonicalName());
        log.info("Set input format {}", titanConf.get(TitanHadoopConfiguration.INPUT_FORMAT));
        titanConf.set(TitanHadoopConfiguration.OUTPUT_FORMAT, NullOutputFormat.class.getCanonicalName());
        log.info("Set output format {}", titanConf.get(TitanHadoopConfiguration.OUTPUT_FORMAT));
        titanConf.set(TitanHadoopConfiguration.SIDE_EFFECT_FORMAT, TextOutputFormat.class.getCanonicalName());
        titanConf.set(TitanHadoopConfiguration.JOBDIR_LOCATION, "jobs");
        titanConf.set(TitanHadoopConfiguration.JOBDIR_OVERWRITE, true);

        ConfigHelper.setInputPartitioner(hadoopConfig, partitioner);

        for (Map.Entry<Object, Object> e : titanProperties.entrySet()) {
            String k;
            String v = e.getValue().toString();
            k = ConfigElement.getPath(TitanHadoopConfiguration.INPUT_CONF_NS) + "." + e.getKey().toString();
            hadoopConfig.set(k, v);
            log.info("Set {}={}", k, v);
            k = ConfigElement.getPath(TitanHadoopConfiguration.OUTPUT_CONF_NS) + "." + e.getKey().toString();
            hadoopConfig.set(k, v);
            log.info("Set {}={}", k, v);
        }

        HadoopGraph hg = new HadoopGraph(hadoopConfig);
        repairIndex(hg);
    }

    public static void main(String args[]) throws Exception {
        HadoopGraph hg = HadoopFactory.open(args[0]);
        repairIndex(hg);
    }

    private static void repairIndex(HadoopGraph hg) throws Exception {
        HadoopCompiler compiler = DEFAULT_COMPAT.newCompiler(hg);
        Class<? extends Mapper<?,?,?,?>> mapper = TitanIndexRepairMapper.class;
        compiler.addMap(mapper, NullWritable.class, NullWritable.class, hg.getConf());
        compiler.completeSequence();
        compiler.run(new String[]{});
        hg.shutdown();
    }
}
