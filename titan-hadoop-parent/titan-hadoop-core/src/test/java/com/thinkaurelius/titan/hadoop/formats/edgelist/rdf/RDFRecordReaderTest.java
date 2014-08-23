package com.thinkaurelius.titan.hadoop.formats.edgelist.rdf;

import com.thinkaurelius.titan.hadoop.BaseTest;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader;

import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RDFRecordReaderTest extends BaseTest {

    public void testRecordReader() throws Exception {
        ModifiableHadoopConfiguration faunusConf = new ModifiableHadoopConfiguration();
        faunusConf.getInputConf(RDFConfig.ROOT_NS).set(RDFConfig.RDF_FORMAT, RDFConfig.Syntax.N_TRIPLES);
        RDFRecordReader reader = new RDFRecordReader(faunusConf);
        reader.initialize(new FileSplit(new Path(RDFRecordReaderTest.class.getResource("graph-example-1.ntriple").toURI()), 0, Long.MAX_VALUE, new String[]{}),
                HadoopCompatLoader.getCompat().newTask(faunusConf.getHadoopConfiguration(), new TaskAttemptID()));
        int counter = 0;
        while (reader.nextKeyValue()) {
            assertEquals(reader.getCurrentKey(), NullWritable.get());
            // reader.getCurrentValue();
            counter++;

        }
        assertEquals(counter, 18 * 3);
        reader.close();
    }
}
