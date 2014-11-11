//package com.thinkaurelius.titan.hadoop.formats.util;
//
//import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
//import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
//import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
//import com.thinkaurelius.titan.util.system.ConfigurationUtil;
//import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
//import org.apache.hadoop.conf.Configurable;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.mapreduce.InputFormat;
//
//import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.TITAN_INPUT_VERSION;
//
//public abstract class AbstractGiraphInputFormat extends InputFormat<NullWritable, GiraphComputeVertex> implements Configurable {
//
//    private static final String SETUP_PACKAGE_PREFIX = "com.thinkaurelius.titan.hadoop.formats.util.input.";
//    private static final String SETUP_CLASS_NAME = ".TitanHadoopSetupImpl";
//
//    protected TitanHadoopSetup titanSetup;
//    protected Configuration hadoopConf;
//    protected ModifiableHadoopConfiguration faunusConf;
//    protected ModifiableConfiguration inputConf;
//
//    @Override
//    public void setConf(final Configuration config) {
//
//        this.faunusConf = ModifiableHadoopConfiguration.of(config);
//        this.inputConf = faunusConf.getInputConf();
//        this.hadoopConf = config;
//
//        final String titanVersion = faunusConf.get(TITAN_INPUT_VERSION);
//        final String className = SETUP_PACKAGE_PREFIX + titanVersion + SETUP_CLASS_NAME;
//
//        this.titanSetup = ConfigurationUtil.instantiate(className, new Object[]{config}, new Class[]{Configuration.class});
//    }
//
//    @Override
//    public Configuration getConf() {
//        return hadoopConf;
//    }
//}
