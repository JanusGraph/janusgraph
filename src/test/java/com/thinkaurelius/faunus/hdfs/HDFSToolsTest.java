package com.thinkaurelius.faunus.hdfs;

import com.thinkaurelius.faunus.BaseTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HDFSToolsTest extends BaseTest {

    /*public void testGetSuffix() {
        assertEquals(HDFSTools.getSuffix("marko.bz2"), "bz2");
        assertEquals(HDFSTools.getSuffix("hdfs://apath/that/is/long/file.gzip"), "gzip");
    }*/

    public void testFileExists() throws IOException {
        FileSystem local = FileSystem.getLocal(new Configuration());
        File root = computeTestDataRoot();
        local.exists(new Path(root.getAbsolutePath()));
    }

    public void testGlob() throws IOException {
        FileSystem local = FileSystem.getLocal(new Configuration());
        File root = computeTestDataRoot();
        new File(root.getAbsolutePath() + "/test.bz2").createNewFile();
        for (FileStatus status : local.globStatus(new Path(root.getAbsolutePath() + "/*"))) {
            assertEquals(status.getPath().getName(), "test.bz2");
        }
    }
}
