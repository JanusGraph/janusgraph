package com.thinkaurelius.titan.hadoop.config;

import com.thinkaurelius.titan.diskstorage.configuration.*;

public class TitanHadoopConfiguration {

    public static final ConfigNamespace SCAN_NS =
            new ConfigNamespace(null, "mrscan", "MapReduce ScanJob configuration root");

    public static final ConfigOption<String> JOB_CONFIG_ROOT =
            new ConfigOption<String>(SCAN_NS, "conf-root",
                    "A string in the form \"PACKAGE.CLASS#STATICFIELD\" representing the config namespace root to use for the ScanJob",
                    ConfigOption.Type.LOCAL, String.class);

    public static final ConfigNamespace JOB_CONFIG_KEYS =
            new ConfigNamespace(SCAN_NS, "conf-keys", "ScanJob configuration");

    public static final ConfigOption<String> JOB_CLASS =
            new ConfigOption<String>(SCAN_NS, "class",
                    "A string in the form \"PACKAGE.CLASS\" representing the ScanJob to use.  Must have a no-arg constructor.",
                    ConfigOption.Type.LOCAL, String.class);

    public static final ConfigNamespace TITAN_INPUT_KEYS =
            new ConfigNamespace(SCAN_NS, "input-conf", "Titan input configuration");

}
