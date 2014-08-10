package com.thinkaurelius.titan.hadoop.formats.util;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.PIPELINE_TRACK_PATHS;
import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.TITAN_INPUT_VERSION;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanInputFormat extends InputFormat<NullWritable, FaunusVertex> implements Configurable {

    private static final String SETUP_PACKAGE_PREFIX = "com.thinkaurelius.titan.hadoop.formats.util.input.";
    private static final String SETUP_CLASS_NAME = ".TitanHadoopSetupImpl";

    protected FaunusVertexQueryFilter vertexQuery;
    protected boolean trackPaths;
    protected TitanHadoopSetup titanSetup;
    protected ModifiableHadoopConfiguration faunusConf;
    protected ModifiableConfiguration titanInputConf;

    @Override
    public void setConf(final Configuration config) {

        this.vertexQuery = FaunusVertexQueryFilter.create(config);

        this.faunusConf = ModifiableHadoopConfiguration.of(config);
        this.titanInputConf = faunusConf.getInputConf();
        final String titanVersion = faunusConf.get(TITAN_INPUT_VERSION);
        this.trackPaths = faunusConf.get(PIPELINE_TRACK_PATHS);
        final String className = SETUP_PACKAGE_PREFIX + titanVersion + SETUP_CLASS_NAME;

        this.titanSetup = ConfigurationUtil.instantiate(className, new Object[]{config}, new Class[]{Configuration.class});
    }
}
