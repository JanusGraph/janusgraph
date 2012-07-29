package com.thinkaurelius.faunus.formats.graphson;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONInputFormatTest extends TestCase {

    public void testSerialization1() throws IOException {
        Configuration config = new Configuration();
        Path file = new Path("target/test1");
        FileSystem fs = FileSystem.get(config);
        fs.copyFromLocalFile(false, true, new Path(GraphSONInputFormatTest.class.getResource("graph-example-1.json").toString()), file);
        assertTrue(fs.exists(file));
        FSDataInputStream in = fs.open(file);
        in.close();
        fs.delete(file, true);
        fs.close();
    }
}
