package com.thinkaurelius.faunus;

import com.thinkaurelius.faunus.formats.Inverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusGraph {

    public static final String GRAPH_INPUT_FORMAT_CLASS = "faunus.graph.input.format.class";
    public static final String INPUT_LOCATION = "faunus.input.location";

    // TODO: data source pre-filters
    // public static final String GRAPH_INPUT_EDGE_DIRECTION_FILTER = "faunus.graph.input.edge.direction.filter";
    // public static final String GRAPH_INPUT_EDGE_LABEL_FILTER_LABELS = "faunus.graph.input.edge.label.filter.labels";
    // public static final String GRAPH_INPUT_EDGE_LABEL_FILTER_ACTION = "faunus.graph.input.edge.label.filter.action";

    public static final String GRAPH_OUTPUT_FORMAT_CLASS = "faunus.graph.output.format.class";
    public static final String STATISTIC_OUTPUT_FORMAT_CLASS = "faunus.statistic.output.format.class";
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

    public Class<? extends InputFormat> getGraphInputFormat() {
        return this.configuration.getClass(GRAPH_INPUT_FORMAT_CLASS, InputFormat.class, InputFormat.class);
    }

    public void setGraphInputFormatClass(final Class<? extends InputFormat> format) {
        this.configuration.setClass(GRAPH_INPUT_FORMAT_CLASS, format, InputFormat.class);
    }

    public Class<? extends OutputFormat> getGraphOutputFormat() {
        return this.configuration.getClass(GRAPH_OUTPUT_FORMAT_CLASS, OutputFormat.class, OutputFormat.class);
    }

    public void setGraphOutputFormatClass(final Class<? extends OutputFormat> format) {
        this.configuration.setClass(GRAPH_OUTPUT_FORMAT_CLASS, format, OutputFormat.class);
    }

    public Class<? extends OutputFormat> getStatisticsOutputFormat() {
        return this.configuration.getClass(STATISTIC_OUTPUT_FORMAT_CLASS, OutputFormat.class, OutputFormat.class);
    }

    public Path getInputLocation() {
        return new Path(this.configuration.get(INPUT_LOCATION));
    }

    public void setInputLocation(final Path path) {
        this.configuration.set(INPUT_LOCATION, path.toString());
    }

    public Path getOutputLocation() {
        return new Path(this.configuration.get(OUTPUT_LOCATION));
    }

    public void setOutputLocation(final Path path) {
        this.configuration.set(OUTPUT_LOCATION, path.toString());
    }

    public boolean getOutputLocationOverwrite() {
        return this.configuration.getBoolean(OUTPUT_LOCATION_OVERWRITE, false);
    }

    public String toString() {
        return "faunusgraph[" + this.configuration.getClass(GRAPH_INPUT_FORMAT_CLASS, InputFormat.class).getSimpleName().toLowerCase() + "]";
    }

    public Map<String, Object> getProperties() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put(GRAPH_INPUT_FORMAT_CLASS, this.configuration.get(GRAPH_INPUT_FORMAT_CLASS));
        map.put(INPUT_LOCATION, this.configuration.get(INPUT_LOCATION));
        map.put(GRAPH_OUTPUT_FORMAT_CLASS, this.configuration.get(GRAPH_OUTPUT_FORMAT_CLASS));
        map.put(STATISTIC_OUTPUT_FORMAT_CLASS, this.configuration.get(STATISTIC_OUTPUT_FORMAT_CLASS));
        map.put(OUTPUT_LOCATION, this.configuration.get(OUTPUT_LOCATION));
        map.put(OUTPUT_LOCATION_OVERWRITE, this.configuration.get(OUTPUT_LOCATION_OVERWRITE));
        return map;
    }

    public FaunusGraph generateInverse() {
        FaunusGraph graph = new FaunusGraph(this.getConfiguration());
        if (null != this.getGraphOutputFormat())
            graph.setGraphInputFormatClass(Inverter.invertOutputFormat(this.getGraphOutputFormat()));
        if (null != this.getOutputLocation()) {
            graph.setInputLocation(this.getOutputLocation());
            graph.setOutputLocation(new Path(this.getOutputLocation().toString() + "_"));
        }

        return graph;
    }
}
