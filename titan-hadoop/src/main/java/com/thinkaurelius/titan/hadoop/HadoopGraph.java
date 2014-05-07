package com.thinkaurelius.titan.hadoop;

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
public class HadoopGraph implements Configurable {

    public static final String TITAN_HADOOP_GRAPH_INPUT_FORMAT = "titan.hadoop.graph.input.format";
    public static final String TITAN_HADOOP_INPUT_LOCATION = "titan.hadoop.input.location";

    public static final String TITAN_HADOOP_GRAPH_OUTPUT_FORMAT = "titan.hadoop.graph.output.format";
    public static final String TITAN_HADOOP_SIDEEFFECT_OUTPUT_FORMAT = "titan.hadoop.sideeffect.output.format";
    public static final String TITAN_HADOOP_OUTPUT_LOCATION = "titan.hadoop.output.location";
    public static final String TITAN_HADOOP_OUTPUT_LOCATION_OVERWRITE = "titan.hadoop.output.location.overwrite";

    private Configuration configuration;

    public HadoopGraph() {
        this(new Configuration());
    }

    public HadoopGraph(final Configuration configuration) {
        this.configuration = new Configuration(configuration);
    }

    public Configuration getConf() {
        return this.configuration;
    }

    public Configuration getConf(final String prefix) {
        final Configuration prefixConf = new EmptyConfiguration();
        final Iterator<Map.Entry<String, String>> itty = this.configuration.iterator();
        while (itty.hasNext()) {
            final Map.Entry<String, String> entry = itty.next();
            if (entry.getKey().startsWith(prefix + "."))
                prefixConf.set(entry.getKey(), entry.getValue());
        }
        return prefixConf;
    }

    public void setConf(final Configuration configuration) {
        this.configuration = configuration;
    }

    // GRAPH INPUT AND OUTPUT FORMATS

    public Class<? extends InputFormat> getGraphInputFormat() {
        return this.configuration.getClass(TITAN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class, InputFormat.class);
    }

    public void setGraphInputFormat(final Class<? extends InputFormat> format) {
        this.configuration.setClass(TITAN_HADOOP_GRAPH_INPUT_FORMAT, format, InputFormat.class);
    }

    public Class<? extends OutputFormat> getGraphOutputFormat() {
        return this.configuration.getClass(TITAN_HADOOP_GRAPH_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
    }

    public void setGraphOutputFormat(final Class<? extends OutputFormat> format) {
        this.configuration.setClass(TITAN_HADOOP_GRAPH_OUTPUT_FORMAT, format, OutputFormat.class);
    }

    // SIDE-EFFECT OUTPUT FORMAT

    public Class<? extends OutputFormat> getSideEffectOutputFormat() {
        return this.configuration.getClass(TITAN_HADOOP_SIDEEFFECT_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
    }

    public void setSideEffectOutputFormat(final Class<? extends OutputFormat> format) {
        this.configuration.setClass(TITAN_HADOOP_SIDEEFFECT_OUTPUT_FORMAT, format, OutputFormat.class);
    }

    // INPUT AND OUTPUT LOCATIONS

    public Path getInputLocation() {
        if (null == this.configuration.get(TITAN_HADOOP_INPUT_LOCATION))
            return null;
        return new Path(this.configuration.get(TITAN_HADOOP_INPUT_LOCATION));
    }

    public void setInputLocation(final Path path) {
        this.configuration.set(TITAN_HADOOP_INPUT_LOCATION, path.toString());
    }

    public void setInputLocation(final String path) {
        this.setInputLocation(new Path(path));
    }

    public Path getOutputLocation() {
        if (null == this.configuration.get(TITAN_HADOOP_OUTPUT_LOCATION))
            throw new IllegalStateException("Please set " + TITAN_HADOOP_OUTPUT_LOCATION + " configuration option.");

        return new Path(this.configuration.get(TITAN_HADOOP_OUTPUT_LOCATION));
    }

    public void setOutputLocation(final Path path) {
        this.configuration.set(TITAN_HADOOP_OUTPUT_LOCATION, path.toString());
    }

    public void setOutputLocation(final String path) {
        this.setOutputLocation(new Path(path));
    }

    public boolean getOutputLocationOverwrite() {
        return this.configuration.getBoolean(TITAN_HADOOP_OUTPUT_LOCATION_OVERWRITE, false);
    }

    public void setOutputLocationOverwrite(final boolean overwrite) {
        this.configuration.setBoolean(TITAN_HADOOP_OUTPUT_LOCATION_OVERWRITE, overwrite);
    }

    public void setTrackPaths(final boolean trackPaths) {
        this.configuration.setBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, trackPaths);
    }

    public boolean getTrackPaths() {
        return this.configuration.getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_PATHS, false);
    }

    public void setTrackState(final boolean trackState) {
        this.configuration.setBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_STATE, trackState);
    }

    public boolean getTrackState() {
        return this.configuration.getBoolean(Tokens.TITAN_HADOOP_PIPELINE_TRACK_STATE, false);
    }

    public void shutdown() {
        this.configuration.clear();
    }

    public String toString() {
        return "titangraph[hadoop:" + this.configuration.getClass(TITAN_HADOOP_GRAPH_INPUT_FORMAT, InputFormat.class).getSimpleName().toLowerCase() + "->" + this.configuration.getClass(TITAN_HADOOP_GRAPH_OUTPUT_FORMAT, OutputFormat.class).getSimpleName().toLowerCase() + "]";
    }

    public HadoopGraph getNextGraph() throws IOException {
        HadoopGraph graph = new HadoopGraph(this.getConf());
        if (null != this.getGraphOutputFormat())
            graph.setGraphInputFormat(Inverter.invertOutputFormat(this.getGraphOutputFormat()));
        if (null != this.getOutputLocation()) {
            graph.setInputLocation(HDFSTools.getOutputsFinalJob(FileSystem.get(this.configuration), this.getOutputLocation().toString()));
            graph.setOutputLocation(new Path(this.getOutputLocation().toString() + "_"));
        }
        return graph;
    }
}
