package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.mapreduce.FaunusCompiler;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SchemaTools {

    public static Configuration readSchema(final Configuration configuration) throws IOException {
        // read the schema configuration in
        final Configuration schema = new EmptyConfiguration();
        schema.readFields(FileSystem.get(configuration).open(new Path(configuration.get("mapred.input.dir") + "/_schema.dat")));
        return schema;
    }

    public static void writeSchema(final Configuration configuration) throws IOException {
        // write the schema configuration out
        final FileSystem fs = FileSystem.get(configuration);
        final Configuration schema = new EmptyConfiguration();
        schema.setBoolean(FaunusCompiler.PATH_ENABLED, configuration.getBoolean(FaunusCompiler.PATH_ENABLED, false));
        schema.write(fs.create(new Path(configuration.get("mapred.output.dir") + "/_schema.dat"), true));
    }
}
