package com.thinkaurelius.faunus;

import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusConfiguration extends Configuration {

    public static final String GRAPH_INPUT_FORMAT_CLASS = "faunus.graph.input.format.class";
    public static final String INPUT_LOCATION = "faunus.input.location";
    // data source pre-filters
    /*public static final String GRAPH_INPUT_EDGE_DIRECTION_FILTER = "faunus.graph.input.edge.direction.filter";
    public static final String GRAPH_INPUT_EDGE_LABEL_FILTER_LABELS = "faunus.graph.input.edge.label.filter.labels";
    public static final String GRAPH_INPUT_EDGE_LABEL_FILTER_ACTION = "faunus.graph.input.edge.label.filter.action";*/

    public static final String GRAPH_OUTPUT_FORMAT_CLASS = "faunus.graph.output.format.class";
    public static final String STATISTIC_OUTPUT_FORMAT_CLASS = "faunus.statistic.output.format.class";
    public static final String OUTPUT_LOCATION = "faunus.output.location";
    public static final String OUTPUT_LOCATION_OVERWRITE = "faunus.output.location.overwrite";

    public FaunusConfiguration(final Configuration configuration) {
        super(configuration);
    }

    public Class<? extends InputFormat> getGraphInputFormat() {
        return this.getClass(GRAPH_INPUT_FORMAT_CLASS, InputFormat.class, InputFormat.class);
    }

    public Class<? extends OutputFormat> getGraphOutputFormat() {
        return this.getClass(GRAPH_OUTPUT_FORMAT_CLASS, OutputFormat.class, OutputFormat.class);
    }

    public Class<? extends OutputFormat> getStatisticsOutputFormat() {
        return this.getClass(STATISTIC_OUTPUT_FORMAT_CLASS, OutputFormat.class, OutputFormat.class);
    }

    public Path getInputLocation() {
        return new Path(this.get(INPUT_LOCATION));
    }

    public Path getOutputLocation() {
        return new Path(this.get(OUTPUT_LOCATION));
    }

    public boolean getOutputLocationOverwrite() {
        return this.getBoolean(OUTPUT_LOCATION_OVERWRITE, false);
    }

    public static Collection<Long> getLongCollection(final Configuration conf, final String key, final Collection<Long> collection) {
        for (final String value : conf.getStrings(key)) {
            collection.add(Long.valueOf(value));
        }
        return collection;
    }
}
