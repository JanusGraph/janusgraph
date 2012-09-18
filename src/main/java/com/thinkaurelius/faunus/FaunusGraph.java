package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.Inverter;
import com.thinkaurelius.faunus.hdfs.HDFSTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusGraph {

    public static final String GRAPH_INPUT_FORMAT = "faunus.graph.input.format";
    public static final String INPUT_LOCATION = "faunus.input.location";

    // TODO: data source pre-filters
    // public static final String GRAPH_INPUT_EDGE_DIRECTION_FILTER = "faunus.graph.input.edge.direction.filter";
    // public static final String GRAPH_INPUT_EDGE_LABEL_FILTER_LABELS = "faunus.graph.input.edge.label.filter.labels";
    // public static final String GRAPH_INPUT_EDGE_LABEL_FILTER_ACTION = "faunus.graph.input.edge.label.filter.action";

    public static final String GRAPH_OUTPUT_FORMAT = "faunus.graph.output.format";
    public static final String SIDEEFFECT_OUTPUT_FORMAT = "faunus.sideeffect.output.format";
    public static final String OUTPUT_LOCATION = "faunus.output.location";
    public static final String OUTPUT_LOCATION_OVERWRITE = "faunus.output.location.overwrite";

    private final Configuration configuration;

    public FaunusGraph() {
        this(new Configuration());
    }

    public FaunusGraph(final Configuration configuration) {
        this.configuration = new Configuration(configuration);
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    // GRAPH INPUT AND OUTPUT FORMATS

    public Class<? extends InputFormat> getGraphInputFormat() {
        return this.configuration.getClass(GRAPH_INPUT_FORMAT, InputFormat.class, InputFormat.class);
    }

    public void setGraphInputFormat(final Class<? extends InputFormat> format) {
        this.configuration.setClass(GRAPH_INPUT_FORMAT, format, InputFormat.class);
    }

    public Class<? extends OutputFormat> getGraphOutputFormat() {
        return this.configuration.getClass(GRAPH_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
    }

    public void setGraphOutputFormat(final Class<? extends OutputFormat> format) {
        this.configuration.setClass(GRAPH_OUTPUT_FORMAT, format, OutputFormat.class);
    }

    // SIDE-EFFECT OUTPUT FORMAT

    public Class<? extends OutputFormat> getSideEffectOutputFormat() {
        return this.configuration.getClass(SIDEEFFECT_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
    }

    public void setSideEffectOutputFormat(final Class<? extends OutputFormat> format) {
        this.configuration.setClass(SIDEEFFECT_OUTPUT_FORMAT, format, OutputFormat.class);
    }

    // INPUT AND OUTPUT LOCATIONS

    public Path getInputLocation() {
        return new Path(this.configuration.get(INPUT_LOCATION));
    }

    public void setInputLocation(final Path path) {
        this.configuration.set(INPUT_LOCATION, path.toString());
    }

    public void setInputLocation(final String path) {
        this.setInputLocation(new Path(path));
    }

    public Path getOutputLocation() {
        return new Path(this.configuration.get(OUTPUT_LOCATION));
    }

    public void setOutputLocation(final Path path) {
        this.configuration.set(OUTPUT_LOCATION, path.toString());
    }

    public void setOutputLocation(final String path) {
        this.setOutputLocation(new Path(path));
    }

    public boolean getOutputLocationOverwrite() {
        return this.configuration.getBoolean(OUTPUT_LOCATION_OVERWRITE, false);
    }

    public void setOutputLocationOverwrite(final boolean overwrite) {
        this.configuration.setBoolean(OUTPUT_LOCATION_OVERWRITE, overwrite);
    }

    public String toString() {
        return "faunusgraph[" + this.configuration.getClass(GRAPH_INPUT_FORMAT, InputFormat.class).getSimpleName().toLowerCase() + "]";
    }

    public Map<String, Object> getProperties() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put(GRAPH_INPUT_FORMAT, this.configuration.get(GRAPH_INPUT_FORMAT));
        map.put(INPUT_LOCATION, this.configuration.get(INPUT_LOCATION));
        map.put(GRAPH_OUTPUT_FORMAT, this.configuration.get(GRAPH_OUTPUT_FORMAT));
        map.put(SIDEEFFECT_OUTPUT_FORMAT, this.configuration.get(SIDEEFFECT_OUTPUT_FORMAT));
        map.put(OUTPUT_LOCATION, this.configuration.get(OUTPUT_LOCATION));
        map.put(OUTPUT_LOCATION_OVERWRITE, this.configuration.getBoolean(OUTPUT_LOCATION_OVERWRITE, false));
        return map;
    }

    public FaunusGraph getNextGraph() throws IOException {
        FaunusGraph graph = new FaunusGraph(this.getConfiguration());
        if (null != this.getGraphOutputFormat())
            graph.setGraphInputFormat(Inverter.invertOutputFormat(this.getGraphOutputFormat()));
        if (null != this.getOutputLocation()) {
            graph.setInputLocation(HDFSTools.getOutputsFinalJob(FileSystem.get(this.configuration), this.getOutputLocation().toString()));
            graph.setOutputLocation(new Path(this.getOutputLocation().toString() + "_"));
        }

        return graph;
    }
}
