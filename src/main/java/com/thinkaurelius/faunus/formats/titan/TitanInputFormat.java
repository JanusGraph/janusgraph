package com.thinkaurelius.faunus.formats.titan;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.VertexQueryFilter;
import com.thinkaurelius.faunus.formats.titan.input.TitanFaunusSetup;
import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TitanInputFormat extends InputFormat<NullWritable, FaunusVertex> implements Configurable {

    public static final String FAUNUS_GRAPH_INPUT_TITAN = "faunus.graph.input.titan";
    public static final String FAUNUS_GRAPH_INPUT_TITAN_STORAGE_HOSTNAME = FAUNUS_GRAPH_INPUT_TITAN + ".storage.hostname";
    public static final String FAUNUS_GRAPH_INPUT_TITAN_STORAGE_PORT = FAUNUS_GRAPH_INPUT_TITAN + ".storage.port";

    public static final String FAUNUS_GRAPH_INPUT_TITAN_VERION = FAUNUS_GRAPH_INPUT_TITAN + ".version";
    public static final String FAUNUS_GRAPH_INPUT_TITAN_VERION_DEFAULT = "current";

    private static final String SETUP_PACKAGE_PREFIX = "com.thinkaurelius.faunus.formats.titan.input.";
    private static final String SETUP_CLASS_NAME = ".TitanFaunusSetupImpl";


    protected VertexQueryFilter vertexQuery;
    protected boolean pathEnabled;
    protected TitanFaunusSetup titanSetup;

    @Override
    public void setConf(final Configuration config) {
        this.vertexQuery = VertexQueryFilter.create(config);
        this.pathEnabled = config.getBoolean(FaunusCompiler.PATH_ENABLED, false);

        String titanVersion = config.get(FAUNUS_GRAPH_INPUT_TITAN_VERION,FAUNUS_GRAPH_INPUT_TITAN_VERION_DEFAULT);
        String className = SETUP_PACKAGE_PREFIX + titanVersion + SETUP_CLASS_NAME;

        titanSetup = ConfigurationUtil.instantiate(className, new Object[]{config}, new Class[]{Configuration.class});
    }


}
