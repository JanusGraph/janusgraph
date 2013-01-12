package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.formats.graphson.GraphSONInputFormat;
import com.thinkaurelius.faunus.formats.graphson.GraphSONOutputFormat;
import com.thinkaurelius.faunus.formats.titan.cassandra.TitanCassandraInputFormat;
import com.thinkaurelius.faunus.formats.titan.cassandra.TitanCassandraOutputFormat;
import com.thinkaurelius.faunus.formats.titan.hbase.TitanHBaseInputFormat;
import com.thinkaurelius.faunus.formats.titan.hbase.TitanHBaseOutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Inverter {

    protected Inverter() {
    }

    public static Class<? extends OutputFormat> invertInputFormat(Class<? extends InputFormat> inputFormat) {
        if (inputFormat.equals(GraphSONInputFormat.class))
            return GraphSONOutputFormat.class;
        else if (inputFormat.equals(SequenceFileInputFormat.class))
            return SequenceFileOutputFormat.class;
        else if (inputFormat.equals(TitanHBaseInputFormat.class))
            return TitanHBaseOutputFormat.class;
        else if (inputFormat.equals(TitanCassandraInputFormat.class))
            return TitanCassandraOutputFormat.class;

        throw new UnsupportedOperationException("There currently is no inverse for " + inputFormat.getName());
    }

    public static Class<? extends InputFormat> invertOutputFormat(Class<? extends OutputFormat> outputFormat) {
        if (outputFormat.equals(GraphSONOutputFormat.class))
            return GraphSONInputFormat.class;
        else if (outputFormat.equals(SequenceFileOutputFormat.class))
            return SequenceFileInputFormat.class;
        else if (outputFormat.equals(TitanHBaseOutputFormat.class))
            return TitanHBaseInputFormat.class;
        else if (outputFormat.equals(TitanCassandraOutputFormat.class))
            return TitanCassandraInputFormat.class;


        throw new UnsupportedOperationException("There currently is no inverse for " + outputFormat.getName());
    }
}
