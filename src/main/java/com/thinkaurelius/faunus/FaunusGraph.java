package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.Inverter;
import com.thinkaurelius.faunus.hdfs.HDFSTools;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
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
public class FaunusGraph implements Configurable {

    public static final String FAUNUS_GRAPH_INPUT_FORMAT = "faunus.graph.input.format";
    public static final String FAUNUS_INPUT_LOCATION = "faunus.input.location";

    public static final String FAUNUS_GRAPH_OUTPUT_FORMAT = "faunus.graph.output.format";
    public static final String FAUNUS_SIDEEFFECT_OUTPUT_FORMAT = "faunus.sideeffect.output.format";
    public static final String FAUNUS_OUTPUT_LOCATION = "faunus.output.location";
    public static final String FAUNUS_OUTPUT_LOCATION_OVERWRITE = "faunus.output.location.overwrite";

    private Configuration configuration;

    public FaunusGraph() {
        this(new Configuration());
    }

    public FaunusGraph(final Configuration configuration) {
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
        return this.configuration.getClass(FAUNUS_GRAPH_INPUT_FORMAT, InputFormat.class, InputFormat.class);
    }

    public void setGraphInputFormat(final Class<? extends InputFormat> format) {
        this.configuration.setClass(FAUNUS_GRAPH_INPUT_FORMAT, format, InputFormat.class);
    }

    public Class<? extends OutputFormat> getGraphOutputFormat() {
        return this.configuration.getClass(FAUNUS_GRAPH_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
    }

    public void setGraphOutputFormat(final Class<? extends OutputFormat> format) {
        this.configuration.setClass(FAUNUS_GRAPH_OUTPUT_FORMAT, format, OutputFormat.class);
    }

    // SIDE-EFFECT OUTPUT FORMAT

    public Class<? extends OutputFormat> getSideEffectOutputFormat() {
        return this.configuration.getClass(FAUNUS_SIDEEFFECT_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
    }

    public void setSideEffectOutputFormat(final Class<? extends OutputFormat> format) {
        this.configuration.setClass(FAUNUS_SIDEEFFECT_OUTPUT_FORMAT, format, OutputFormat.class);
    }

    // INPUT AND OUTPUT LOCATIONS

    public Path getInputLocation() {
        if (null == this.configuration.get(FAUNUS_INPUT_LOCATION))
            return null;
        return new Path(this.configuration.get(FAUNUS_INPUT_LOCATION));
    }

    public void setInputLocation(final Path path) {
        this.configuration.set(FAUNUS_INPUT_LOCATION, path.toString());
    }

    public void setInputLocation(final String path) {
        this.setInputLocation(new Path(path));
    }

    public Path getOutputLocation() {
        if (null == this.configuration.get(FAUNUS_OUTPUT_LOCATION))
            throw new IllegalStateException("Please set " + FAUNUS_OUTPUT_LOCATION + " configuration option.");

        return new Path(this.configuration.get(FAUNUS_OUTPUT_LOCATION));
    }

    public void setOutputLocation(final Path path) {
        this.configuration.set(FAUNUS_OUTPUT_LOCATION, path.toString());
    }

    public void setOutputLocation(final String path) {
        this.setOutputLocation(new Path(path));
    }

    public boolean getOutputLocationOverwrite() {
        return this.configuration.getBoolean(FAUNUS_OUTPUT_LOCATION_OVERWRITE, false);
    }

    public void setOutputLocationOverwrite(final boolean overwrite) {
        this.configuration.setBoolean(FAUNUS_OUTPUT_LOCATION_OVERWRITE, overwrite);
    }

    public void shutdown() {
        this.configuration.clear();
    }

    public String toString() {
        return "faunusgraph[" + this.configuration.getClass(FAUNUS_GRAPH_INPUT_FORMAT, InputFormat.class).getSimpleName().toLowerCase() + "->" + this.configuration.getClass(FAUNUS_GRAPH_OUTPUT_FORMAT, OutputFormat.class).getSimpleName().toLowerCase() + "]";
    }

    public FaunusGraph getNextGraph() throws IOException {
        FaunusGraph graph = new FaunusGraph(this.getConf());
        if (null != this.getGraphOutputFormat())
            graph.setGraphInputFormat(Inverter.invertOutputFormat(this.getGraphOutputFormat()));
        if (null != this.getOutputLocation()) {
            graph.setInputLocation(HDFSTools.getOutputsFinalJob(FileSystem.get(this.configuration), this.getOutputLocation().toString()));
            graph.setOutputLocation(new Path(this.getOutputLocation().toString() + "_"));
        }

        /*
        TODO: This needs to be put into the "input handler" system
        final Iterator<Map.Entry<String, String>> itty = this.configuration.iterator();
        while (itty.hasNext()) {
            final Map.Entry<String, String> entry = itty.next();
            if (entry.getKey().startsWith("faunus.graph.output.titan."))  {
                configuration.set("faunus.graph.input.titan." + entry.getKey().substring("faunus.graph.output.titan.".length()+1), entry.getValue());
            }
        }*/

        return graph;
    }
}
