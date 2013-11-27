package com.thinkaurelius.titan.pkgtest;

import static org.junit.Assert.assertEquals;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.Template;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.MethodInvocationException;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import java.io.Writer;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.PrintStream;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.thinkaurelius.titan.core.TitanFactory;

public class AssemblyITSupport {
    
    protected static final String BUILD_DIR;
    protected static final String EXPECT_DIR;
    protected static final String ZIPFILE_PATH;
    protected static final String ZIPFILE_EXTRACTED;
    
    static {
        Properties props;

        try {
            props = new Properties();
            props.load(TitanFactory.class.getClassLoader().getResourceAsStream("target.properties"));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        
        BUILD_DIR    = props.getProperty("build.dir");
        EXPECT_DIR   = props.getProperty("expect.dir");
        ZIPFILE_PATH = props.getProperty("zipfile.path");
        ZIPFILE_EXTRACTED = ZIPFILE_PATH.substring(0, ZIPFILE_PATH.length() - 4);

        Properties p = new Properties();
        p.put("file.resource.loader.path", EXPECT_DIR);
        Velocity.init(p);
    }
    
    protected void testSimpleGremlinSession(String graphConfig, String graphToString) throws Exception {
        unzipAndRunExpect("single-vertex.expect.vm", graphConfig, graphToString);
    }
    
    protected void testGettingStartedGremlinSession(String graphConfig, String graphToString) throws Exception {
        unzipAndRunExpect("getting-started.expect.vm", graphConfig, graphToString);
    }
    
    private void unzipAndRunExpect(String expectTemplateName, String graphConfig, String graphToString) throws Exception {
        FileUtils.deleteQuietly(new File(ZIPFILE_EXTRACTED));
        unzip(BUILD_DIR, ZIPFILE_PATH);
        
        VelocityContext context = new VelocityContext();
        context.put("graphConfig", graphConfig);
        context.put("graphToString", graphToString);
        context.put("graphPostamble", "");
        
        Template template = Velocity.getTemplate(expectTemplateName);
        String inputPath = EXPECT_DIR + File.separator + expectTemplateName;
        String outputPath = inputPath.substring(0, inputPath.length() - 3);
        
        Writer output = new FileWriter(outputPath);
        template.merge(context, output);
        output.close();
        
        expect(ZIPFILE_EXTRACTED, outputPath);
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
//        pb.redirectInput(Redirect.PIPE);
        /*
         * Using Redirect.INHERIT with expect breaks maven-failsafe-plugin when
         * failsafe is configured to fork. The parent and child normally
         * communicate over stdout/stderr in fork mode. But after executing
         * expect, this failsafe communication starts appearing on the terminal.
         * The parent never sees it and assumes no tests ran. expect is probably
         * doing something nasty to its file descriptors and neglecting to clean
         * up after itself. Invoking unzip does not break failsafe+forks in this
         * way. So expect must be doing something unusual with its file
         * descriptors.
         * 
         * Redirect.INHERIT works fine if failsafe is configured to never fork.
         */
//        pb.redirectOutput(Redirect.INHERIT);
//        pb.redirectError(Redirect.INHERIT);
//        pb.redirectOutput(Redirect.PIPE);
//        pb.redirectError(Redirect.PIPE);
        final Process p = pb.start();
        // Sense of "input" and "output" are reversed between ProcessBuilder and Process
        p.getOutputStream().close(); // Child process sees EOF on stdin (if it reads stdin at all)
        Thread outPrinter = new Thread(new SubprocessPipePrinter(p.getInputStream(), System.out));
        Thread errPrinter = new Thread(new SubprocessPipePrinter(p.getErrorStream(), System.out));
        outPrinter.start();
        errPrinter.start();
        int stat = p.waitFor();
        outPrinter.join();
        errPrinter.join();
        assertEquals(0, stat);
    }
    
    private static class SubprocessPipePrinter implements Runnable {
        
        private final BufferedReader source;
        private final PrintStream sink;
        
        private SubprocessPipePrinter(InputStream source, PrintStream sink) {
            this.source = new BufferedReader(new InputStreamReader(source));
            this.sink = sink;
        }
            
        @Override
        public void run() {
            try {
                runUnsafe();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // Exit silently
            }
        }
        
        private void runUnsafe() throws IOException, InterruptedException {
            
            String line = null;
            
            while (null != (line = source.readLine())) {
                synchronized (sink) {
                    sink.println(line);
                }
            }
        }
    }
}
