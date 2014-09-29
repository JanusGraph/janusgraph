package com.thinkaurelius.titan.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.hadoop.config.HBaseAuthHelper;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

import com.thinkaurelius.titan.hadoop.config.HybridConfigured;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.Inverter;
import com.thinkaurelius.titan.hadoop.hdfs.HDFSTools;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopGraph extends HybridConfigured {

    public HadoopGraph() {
        this(new Configuration());
    }

    public HadoopGraph(final Configuration configuration) {
        super(HBaseAuthHelper.wrapConfiguration(configuration));
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
        return titanConf.getClass(INPUT_FORMAT, InputFormat.class, InputFormat.class);
    }

    public void setGraphInputFormat(final Class<? extends InputFormat> format) {
        titanConf.setClass(INPUT_FORMAT, format, InputFormat.class);
    }

    public Class<? extends OutputFormat> getGraphOutputFormat() {
        return titanConf.getClass(OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
    }

    public void setGraphOutputFormat(final Class<? extends OutputFormat<?,?>> format) {
        titanConf.setClass(OUTPUT_FORMAT, format, OutputFormat.class);
    }

    // SIDE-EFFECT OUTPUT FORMAT

    public Class<? extends OutputFormat> getSideEffectOutputFormat() {
        return titanConf.getClass(SIDE_EFFECT_FORMAT, OutputFormat.class, OutputFormat.class);
    }

    public void setSideEffectOutputFormat(final Class<? extends OutputFormat<?,?>> format) {
        titanConf.setClass(SIDE_EFFECT_FORMAT, format, OutputFormat.class);
    }

    // INPUT AND OUTPUT LOCATIONS

    public Path getInputLocation() {
        if (!getTitanConf().has(INPUT_LOCATION))
            return null;

        return new Path(getTitanConf().get(INPUT_LOCATION));
    }

    public void setInputLocation(final Path path) {
        getTitanConf().set(INPUT_LOCATION, path.toString());
    }

    public void setInputLocation(final String path) {
        this.setInputLocation(new Path(path));
    }

    // Edge copy

    public boolean hasEdgeCopyDirection() {
        ModifiableConfiguration mc = getTitanConf();
        return mc.has(INPUT_EDGE_COPY_DIR) || mc.has(INPUT_EDGE_COPY_DIRECTION);
    }

    public Direction getEdgeCopyDirection() {
        return getTitanConf().getEdgeCopyDirection();
    }

    // JOB AND FILESYSTEM

    public Path getJobDir() {
        return new Path(getTitanConf().get(JOBDIR_LOCATION));
    }

    public void setJobDir(final Path path) {
        getTitanConf().set(JOBDIR_LOCATION, path.toString());
    }

    public void setJobDir(final String path) {
        setJobDir(new Path(path));
    }

    public boolean getJobDirOverwrite() {
        return getTitanConf().get(JOBDIR_OVERWRITE);
    }

    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(getConf());
    }

    public boolean getTrackPaths() {
        return getTitanConf().get(PIPELINE_TRACK_PATHS);
    }

    public boolean getTrackState() {
        return getTitanConf().get(PIPELINE_TRACK_STATE);
    }

    public void shutdown() {
        clearConfiguration();
    }

    public String toString() {
        return String.format("titangraph[hadoop:%s->%s]",
            titanConf.getClass(TitanHadoopConfiguration.INPUT_FORMAT, InputFormat.class).getSimpleName().toLowerCase(),
            titanConf.getClass(TitanHadoopConfiguration.OUTPUT_FORMAT, OutputFormat.class).getSimpleName().toLowerCase());
    }

    public HadoopGraph getNextGraph() throws IOException {
        HadoopGraph graph = new HadoopGraph(this.getConf());
        if (null != getGraphOutputFormat())
            graph.setGraphInputFormat(Inverter.invertOutputFormat(getGraphOutputFormat()));
        if (null != getJobDir()) {
            graph.setInputLocation(HDFSTools.getOutputsFinalJob(FileSystem.get(getConf()), getJobDir().toString()));
            graph.setJobDir(new Path(getJobDir().toString() + "_"));
        }
        return graph;
    }
}
