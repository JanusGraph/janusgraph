package com.thinkaurelius.titan.hadoop.formats.script;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;

public class ScriptConfig {

    public static final ConfigNamespace ROOT_NS =
            new ConfigNamespace(null, "script", "ScriptInput/Output format options");

    public static final ConfigOption<String> SCRIPT_FILE =
            new ConfigOption<String>(ROOT_NS, "file",
            "Gremlin script file which defines a read and/or write method for " +
            "ScriptInputFormat and/or ScriptOutputFormat",
            ConfigOption.Type.LOCAL, String.class);
}
