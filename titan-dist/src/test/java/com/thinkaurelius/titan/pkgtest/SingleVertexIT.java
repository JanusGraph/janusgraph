package com.thinkaurelius.titan.pkgtest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.Properties;

import org.junit.Test;

import com.thinkaurelius.titan.core.TitanFactory;

public class SingleVertexIT {
    
    private static final String ZIPFILE_PATH;
    private static final String EXPECT_DIR;
    private static final String BUILD_DIR;
    
    static {
        Properties props;

        try {
            props = new Properties();
            props.load(TitanFactory.class.getClassLoader().getResourceAsStream("target.properties"));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        
        ZIPFILE_PATH = props.getProperty("zipfile.path");
        EXPECT_DIR   = props.getProperty("expect.dir");
        BUILD_DIR    = props.getProperty("build.dir");
    }
    
    @Test
    public void testSimpleGremlinSession() throws IOException, InterruptedException {
        unzip(BUILD_DIR, ZIPFILE_PATH);
        String unzippedDir = ZIPFILE_PATH.substring(0, ZIPFILE_PATH.length() - 4);
        expect(unzippedDir, EXPECT_DIR + File.separator + "single-vertex.expect");
    }
    
    private static void expect(String dir, String expectScript) throws IOException, InterruptedException {
        command(new File(dir), "expect", expectScript);
    }
    
    private static void unzip(String dir, String zipfile) throws IOException, InterruptedException {
        command(new File(dir), "unzip", "-q", zipfile);
    }
    
    private static void command(File dir, String... command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(dir);
        pb.redirectInput(Redirect.PIPE);
        /*
         * Using Redirect.INHERIT with expect breaks maven-failsafe-plugin when
         * failsafe is configured to fork. After running a test that invokes
         * expect, the child process running maven-failsafe-plugin echoes
         * failsafe-internal test result strings to the console instead of
         * through a pipe to its parent. The parent never sees these strings and
         * assumes no tests ran. expect is probably doing something nasty to its
         * file descriptors and neglecting to clean up after itself. Invoking
         * unzip does not break failsafe+forks in this way. So expect must be
         * doing something bad.
         * 
         * Redirect.INHERIT works fine if failsafe is configured to never fork.
         */
//        pb.redirectOutput(Redirect.appendTo(new File("crap.stdout")));
//        pb.redirectError(Redirect.appendTo(new File("crap.stderr")));
        pb.redirectOutput(Redirect.INHERIT);
        pb.redirectError(Redirect.INHERIT);
        Process p = pb.start();
        // Sense of "input" and "output" are reversed between ProcessBuilder and Process
        p.getOutputStream().close(); // Child zip process sees EOF on stdin (if it reads stdin at all)
        assertEquals(0, p.waitFor());
    }
}
