// Copyright 2020 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.pkgtest;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class JanusGraphAssemblyBaseIT {

    protected static final String BUILD_DIR;
    protected static final String EXPECT_DIR;
    protected static final String ZIPFILE_PATH;
    protected static final String ZIPFILE_EXTRACTED;
    protected static final String ZIPFILE_FULL_PATH;
    protected static final String ZIPFILE_FULL_EXTRACTED;

    static {
        Properties props;

        try {
            props = new Properties();
            java.io.FileReader fr = new FileReader(Joiner.on(File.separator).join(new String[] { "target", "test-classes", "target.properties" }));
            props.load(fr);
            fr.close();
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        BUILD_DIR    = props.getProperty("build.dir");
        EXPECT_DIR   = props.getProperty("expect.dir");
        ZIPFILE_PATH = props.getProperty("zipfile.path");
        ZIPFILE_EXTRACTED = ZIPFILE_PATH.substring(0, ZIPFILE_PATH.length() - 4);
        ZIPFILE_FULL_PATH = props.getProperty("zipfile-full.path");
        ZIPFILE_FULL_EXTRACTED = ZIPFILE_FULL_PATH.substring(0, ZIPFILE_FULL_PATH.length() - 4);

        Properties p = new Properties();
        p.put("file.resource.loader.path", EXPECT_DIR);
        Velocity.init(p);
    }

    protected void unzipAndRunExpect(String expectTemplateName, Map<String, String> contextVars, boolean full, boolean debug) throws Exception {
        if (full) {
            FileUtils.deleteQuietly(new File(ZIPFILE_FULL_EXTRACTED));
            unzip(BUILD_DIR, ZIPFILE_FULL_PATH);
        } else {
            FileUtils.deleteQuietly(new File(ZIPFILE_EXTRACTED));
            unzip(BUILD_DIR, ZIPFILE_PATH);
        }

        parseTemplateAndRunExpect(expectTemplateName, contextVars, full, debug);
    }

    protected void parseTemplateAndRunExpect(String expectTemplateName, Map<String, String> contextVars) throws IOException, InterruptedException {
        parseTemplateAndRunExpect(expectTemplateName, contextVars, true, false);
    }

    protected void parseTemplateAndRunExpect(String expectTemplateName, Map<String, String> contextVars, boolean full, boolean debug) throws IOException, InterruptedException {
        VelocityContext context = new VelocityContext();
        for (Map.Entry<String, String> ent : contextVars.entrySet()) {
            context.put(ent.getKey(), ent.getValue());
        }

        Template template = Velocity.getTemplate(expectTemplateName);
        String inputPath = EXPECT_DIR + File.separator + expectTemplateName;
        String outputPath = inputPath.substring(0, inputPath.length() - 3);

        Writer output = new FileWriter(outputPath);
        template.merge(context, output);
        output.close();

        if (full) {
            expect(ZIPFILE_FULL_EXTRACTED, outputPath, debug);
        } else {
            expect(ZIPFILE_EXTRACTED, outputPath, debug);
        }
    }

    protected void unzipAndRunExpect(String expectTemplateName, boolean full) throws Exception {
        unzipAndRunExpect(expectTemplateName, Collections.emptyMap(), full, false);
    }

    protected void unzipAndRunExpect(String expectTemplateName, String graphConfig, String graphToString, boolean full, boolean debug) throws Exception {
        unzipAndRunExpect(expectTemplateName, ImmutableMap.of("graphConfig", graphConfig, "graphToString", graphToString), full, debug);
    }

    private static void expect(String dir, String expectScript, boolean debug) throws IOException, InterruptedException {
        if (debug) {
            command(new File(dir), "expect", "-d", expectScript);
        } else {
            command(new File(dir), "expect", expectScript);
        }
    }

    protected static void unzip(String dir, String zipFile) throws IOException, InterruptedException {
        command(new File(dir), "unzip", "-q", zipFile);
    }

    protected static void command(File dir, String... command) throws IOException, InterruptedException {
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
            }
        }

        private void runUnsafe() throws IOException {

            String line;

            while (null != (line = source.readLine())) {
                synchronized (sink) {
                    sink.println(line);
                }
            }
        }
    }
}
