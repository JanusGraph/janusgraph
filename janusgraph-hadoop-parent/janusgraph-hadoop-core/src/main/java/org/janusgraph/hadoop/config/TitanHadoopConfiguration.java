package com.thinkaurelius.titan.hadoop.config;

import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.configuration.*;

public class TitanHadoopConfiguration {

    public static final ConfigNamespace MAPRED_NS =
            new ConfigNamespace(null, "titanmr", "Titan MapReduce configuration root");

    // ScanJob configuration

    public static final ConfigNamespace SCAN_NS =
            new ConfigNamespace(MAPRED_NS, "scanjob", "ScanJob configuration");

    public static final ConfigOption<String> SCAN_JOB_CONFIG_ROOT =
            new ConfigOption<>(SCAN_NS, "conf-root",
                    "A string in the form \"PACKAGE.CLASS#STATICFIELD\" representing the config namespace root to use for the ScanJob",
                    ConfigOption.Type.LOCAL, String.class);

    public static final ConfigNamespace SCAN_JOB_CONFIG_KEYS =
            new ConfigNamespace(SCAN_NS, "conf", "ScanJob configuration");

    public static final ConfigOption<String> SCAN_JOB_CLASS =
            new ConfigOption<>(SCAN_NS, "class",
                    "A string in the form \"PACKAGE.CLASS\" representing the ScanJob to use.  Must have a no-arg constructor.",
                    ConfigOption.Type.LOCAL, String.class);

    // Titan Hadoop I/O format configuration

    public static final ConfigNamespace IOFORMAT_NS =
            new ConfigNamespace(MAPRED_NS, "ioformat", "Titan input configuration");

    public static final ConfigNamespace GRAPH_CONFIG_KEYS =
            new ConfigNamespace(IOFORMAT_NS, "conf", "Settings to be passed to TitanFactory.open");

    public static final ConfigOption<Boolean> FILTER_PARTITIONED_VERTICES =
            new ConfigOption<>(IOFORMAT_NS, "filter-partitioned-vertices",
                    "True to drop partitioned vertices and relations incident on partitioned vertices when reading " +
                    "from Titan.  This currently must be true when partitioned vertices are present in the " +
                    "input; if it is false when a partitioned vertex is encountered, then an exception is thrown.  " +
                    "This limitation may be lifted in a later version of Titan-Hadoop.",
                    ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<String> COLUMN_FAMILY_NAME =
            new ConfigOption<>(IOFORMAT_NS, "cf-name",
                    "The name of the column family from which the Hadoop input format should read.  " +
                            "Usually edgestore or graphindex.", ConfigOption.Type.LOCAL, Backend.EDGESTORE_NAME);

    // Titan bulkload vertex program configuration

    public static final ConfigNamespace BULKLOAD_NS =
            new ConfigNamespace(MAPRED_NS, "bulkload", "Titan BulkLoaderVertexProgram configuration");

    public static final ConfigNamespace BULKLOAD_GRAPH_CONFIG_KEYS =
            new ConfigNamespace(BULKLOAD_NS, "conf", "Settings to be passed to TitanFactory.open");

    public static final ConfigOption<Boolean> BULKLOAD_SCHEMA_CHECK =
            new ConfigOption<>(BULKLOAD_NS, "filter-partitioned-vertices",
                    "Whether to enforce best-effort checks on edge multiplicity and property cardinality.  " +
                    "These checks do not read the existing properties and edges in Titan.  They only consider " +
                    "those elements visible from a single MapReduce worker.  Hence, these checks do not " +
                    "guarantee that invalid input data will be detected and rejected.",
                    ConfigOption.Type.LOCAL, false);

}
