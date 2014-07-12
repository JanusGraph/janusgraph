package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.hadoop.config.HybridConfigured;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.Inverter;
import com.thinkaurelius.titan.hadoop.hdfs.HDFSTools;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopGraph extends HybridConfigured {

    public static final String TITAN_HADOOP_GRAPH_INPUT_FORMAT = "titan.hadoop.input.format";
    public static final String TITAN_HADOOP_INPUT_LOCATION = "titan.hadoop.input.location";

    public static final String TITAN_HADOOP_GRAPH_OUTPUT_FORMAT = "titan.hadoop.output.format";
    public static final String TITAN_HADOOP_SIDEEFFECT_OUTPUT_FORMAT = "titan.hadoop.sideeffect.output.format";
    public static final String TITAN_HADOOP_OUTPUT_LOCATION = "titan.hadoop.output.location";
    public static final String TITAN_HADOOP_OUTPUT_LOCATION_OVERWRITE = "titan.hadoop.output.location.overwrite";

//    private Configuration configuration;

    public HadoopGraph() {
        super();
    }

    public HadoopGraph(final Configuration configuration) {
        super(configuration);
    }

    public Configuration getConf(final String prefix) {
        final Configuration prefixConf = new EmptyConfiguration();
        final Iterator<Map.Entry<String, String>> itty = getConf().iterator();
        while (itty.hasNext()) {
            final Map.Entry<String, String> entry = itty.next();
            if (entry.getKey().startsWith(prefix + "."))
                prefixConf.set(entry.getKey(), entry.getValue());
        }
        return prefixConf;
    }

    // GRAPH INPUT AND OUTPUT FORMATS

    public Class<? extends InputFormat> getGraphInputFormat() {
        return getConf().getClass(ConfigElement.getPath(TitanHadoopConfiguration.INPUT_FORMAT), InputFormat.class, InputFormat.class);
    }

    public void setGraphInputFormat(final Class<? extends InputFormat> format) {
        setConfClass(TitanHadoopConfiguration.INPUT_FORMAT, format, InputFormat.class);
    }

    public Class<? extends OutputFormat> getGraphOutputFormat() {
        Class<? extends OutputFormat> cls = getConf().getClass(ConfigElement.getPath(TitanHadoopConfiguration.OUTPUT_FORMAT), OutputFormat.class, OutputFormat.class);
        return cls;
    }

    public void setGraphOutputFormat(final Class<? extends OutputFormat> format) {
        setConfClass(TitanHadoopConfiguration.OUTPUT_FORMAT, format, OutputFormat.class);
    }

    // SIDE-EFFECT OUTPUT FORMAT

    public Class<? extends OutputFormat> getSideEffectOutputFormat() {
        return getConf().getClass(ConfigElement.getPath(TitanHadoopConfiguration.SIDE_EFFECT_FORMAT), OutputFormat.class, OutputFormat.class);
    }

    public void setSideEffectOutputFormat(final Class<? extends OutputFormat> format) {
        setConfClass(TitanHadoopConfiguration.SIDE_EFFECT_FORMAT, format, OutputFormat.class);
    }

    // INPUT AND OUTPUT LOCATIONS

    public Path getInputLocation() {
        if (!getTitanConf().has(TitanHadoopConfiguration.INPUT_LOCATION))
            return null;
        return new Path(getTitanConf().get(TitanHadoopConfiguration.INPUT_LOCATION));
    }

    public void setInputLocation(final Path path) {
        getConf().set(ConfigElement.getPath(TitanHadoopConfiguration.INPUT_LOCATION), path.toString());
    }

    public void setInputLocation(final String path) {
        this.setInputLocation(new Path(path));
    }

    public Path getTemporarySeqFileLocation() {
        if (!getTitanConf().has(TitanHadoopConfiguration.TMP_SEQUENCEFILE_LOCATION))
            return null;

        return new Path(getTitanConf().get(TitanHadoopConfiguration.TMP_SEQUENCEFILE_LOCATION));
    }

    public void setTemporarySeqFileLocation(final Path path) {
        getConf().set(ConfigElement.getPath(TitanHadoopConfiguration.TMP_SEQUENCEFILE_LOCATION), path.toString());
    }

    public void setTemporarySeqFileLocation(final String path) {
        this.setTemporarySeqFileLocation(new Path(path));
    }

    public boolean getTemporarySeqFileOverwrite() {
        return getTitanConf().get(TitanHadoopConfiguration.TMP_SEQUENCEFILE_OVERWRITE);
    }

    public boolean getTrackPaths() {
        return getTitanConf().get(TitanHadoopConfiguration.PIPELINE_TRACK_PATHS);
    }

    public boolean getTrackState() {
        return getTitanConf().get(TitanHadoopConfiguration.PIPELINE_TRACK_STATE);
    }

    public void shutdown() {
        getConf().clear();
    }

    public String toString() {
        return String.format("titangraph[hadoop:%s->%s]",
            getConfClass(TitanHadoopConfiguration.INPUT_FORMAT, InputFormat.class).getSimpleName().toLowerCase(),
            getConfClass(TitanHadoopConfiguration.OUTPUT_FORMAT, OutputFormat.class).getSimpleName().toLowerCase());
    }

    public HadoopGraph getNextGraph() throws IOException {
        HadoopGraph graph = new HadoopGraph(this.getConf());
        if (null != getGraphOutputFormat())
            graph.setGraphInputFormat(Inverter.invertOutputFormat(getGraphOutputFormat()));
        if (null != getTemporarySeqFileLocation()) {
            graph.setInputLocation(HDFSTools.getOutputsFinalJob(FileSystem.get(getConf()), getTemporarySeqFileLocation().toString()));
            graph.setTemporarySeqFileLocation(new Path(getTemporarySeqFileLocation().toString() + "_"));
        }
        return graph;
    }
}
