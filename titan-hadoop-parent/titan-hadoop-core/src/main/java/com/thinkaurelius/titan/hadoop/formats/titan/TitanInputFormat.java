package com.thinkaurelius.titan.hadoop.formats.titan;

import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.hadoop.HadoopVertex;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.VertexQueryFilter;
import com.thinkaurelius.titan.hadoop.formats.titan.input.TitanHadoopSetup;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanInputFormat extends InputFormat<NullWritable, HadoopVertex> implements Configurable {

//    public static final String TITAN_HADOOP_GRAPH_INPUT_TITAN = "titan.hadoop.input";
//    public static final String TITAN_HADOOP_GRAPH_INPUT_TITAN_STORAGE_HOSTNAME = TITAN_HADOOP_GRAPH_INPUT_TITAN + ".storage.hostname";
//    public static final String TITAN_HADOOP_GRAPH_INPUT_TITAN_STORAGE_PORT = TITAN_HADOOP_GRAPH_INPUT_TITAN + ".storage.port";

//    public static final String TITAN_HADOOP_GRAPH_INPUT_TITAN_VERION = TITAN_HADOOP_GRAPH_INPUT_TITAN + ".version";
//    public static final String TITAN_HADOOP_GRAPH_INPUT_TITAN_VERION_DEFAULT = "current";

    private static final String SETUP_PACKAGE_PREFIX = "com.thinkaurelius.titan.hadoop.formats.titan.input.";
    private static final String SETUP_CLASS_NAME = ".TitanHadoopSetupImpl";

    protected VertexQueryFilter vertexQuery;
    protected boolean trackPaths;
    protected TitanHadoopSetup titanSetup;
    protected BasicConfiguration titanConf;

    @Override
    public void setConf(final Configuration config) {

        this.vertexQuery = VertexQueryFilter.create(config);
        this.trackPaths = config.getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, false);
        this.titanConf = com.thinkaurelius.titan.hadoop.config.ConfigurationUtil.extractInputConfiguration(config);

        final String titanVersion =
                com.thinkaurelius.titan.hadoop.config.ConfigurationUtil.get(config, TitanHadoopConfiguration.TITAN_INPUT_VERSION);

        final String className = SETUP_PACKAGE_PREFIX + titanVersion + SETUP_CLASS_NAME;

        this.titanSetup = ConfigurationUtil.instantiate(className, new Object[]{config}, new Class[]{Configuration.class});
    }
}
