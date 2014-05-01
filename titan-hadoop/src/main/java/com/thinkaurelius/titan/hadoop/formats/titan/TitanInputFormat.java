package com.thinkaurelius.titan.hadoop.formats.titan;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.formats.VertexQueryFilter;
import com.thinkaurelius.titan.hadoop.formats.titan.input.TitanFaunusSetup;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanInputFormat extends InputFormat<NullWritable, FaunusVertex> implements Configurable {

    public static final String FAUNUS_GRAPH_INPUT_TITAN = "faunus.graph.input.titan";
    public static final String FAUNUS_GRAPH_INPUT_TITAN_STORAGE_HOSTNAME = FAUNUS_GRAPH_INPUT_TITAN + ".storage.hostname";
    public static final String FAUNUS_GRAPH_INPUT_TITAN_STORAGE_PORT = FAUNUS_GRAPH_INPUT_TITAN + ".storage.port";

    public static final String FAUNUS_GRAPH_INPUT_TITAN_VERION = FAUNUS_GRAPH_INPUT_TITAN + ".version";
    public static final String FAUNUS_GRAPH_INPUT_TITAN_VERION_DEFAULT = "current";

    private static final String SETUP_PACKAGE_PREFIX = "com.thinkaurelius.titan.hadoop.formats.titan.input.";
    private static final String SETUP_CLASS_NAME = ".TitanFaunusSetupImpl";


    protected VertexQueryFilter vertexQuery;
    protected boolean trackPaths;
    protected TitanFaunusSetup titanSetup;

    @Override
    public void setConf(final Configuration config) {
        this.vertexQuery = VertexQueryFilter.create(config);
        this.trackPaths = config.getBoolean(Tokens.FAUNUS_PIPELINE_TRACK_PATHS, false);

        final String titanVersion = config.get(FAUNUS_GRAPH_INPUT_TITAN_VERION, FAUNUS_GRAPH_INPUT_TITAN_VERION_DEFAULT);
        final String className = SETUP_PACKAGE_PREFIX + titanVersion + SETUP_CLASS_NAME;

        this.titanSetup = ConfigurationUtil.instantiate(className, new Object[]{config}, new Class[]{Configuration.class});
    }


}
