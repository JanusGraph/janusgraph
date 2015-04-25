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
            new ConfigOption<String>(MAPRED_NS, "conf-root",
                    "A string in the form \"PACKAGE.CLASS#STATICFIELD\" representing the config namespace root to use for the ScanJob",
                    ConfigOption.Type.LOCAL, String.class);

    public static final ConfigNamespace SCAN_JOB_CONFIG_KEYS =
            new ConfigNamespace(MAPRED_NS, "conf", "ScanJob configuration");

    public static final ConfigOption<String> SCAN_JOB_CLASS =
            new ConfigOption<String>(MAPRED_NS, "class",
                    "A string in the form \"PACKAGE.CLASS\" representing the ScanJob to use.  Must have a no-arg constructor.",
                    ConfigOption.Type.LOCAL, String.class);

    // Titan input configuration

    public static final ConfigNamespace INPUT_NS =
            new ConfigNamespace(MAPRED_NS, "input", "Titan input configuration");

    public static final ConfigNamespace TITAN_INPUT_CONFIG_KEYS =
            new ConfigNamespace(INPUT_NS, "conf", "Settings to be passed to TitanFactory.open");

    public static final ConfigOption<Boolean> FILTER_PARTITIONED_VERTICES =
            new ConfigOption<Boolean>(INPUT_NS, "filter-partitioned-vertices",
                    "True to drop partitioned vertices and relations incident on partitioned vertices when reading " +
                    "from Titan.  This currently must be true when partitioned vertices are present in the " +
                    "input; if it is false when a partitioned vertex is encountered, then an exception is thrown.  " +
                    "This limitation may be lifted in a later version of Titan-Hadoop.",
                    ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<String> COLUMN_FAMILY_NAME =
            new ConfigOption<>(INPUT_NS, "cf-name",
                    "The name of the column family from which the input format should read.  " +
                    "Usually edgestore or graphindex.", ConfigOption.Type.LOCAL, Backend.EDGESTORE_NAME);

}
