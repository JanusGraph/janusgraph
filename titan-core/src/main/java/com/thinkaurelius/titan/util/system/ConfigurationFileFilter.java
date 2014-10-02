package com.thinkaurelius.titan.util.system;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.lang.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigurationFileFilter {

    private static final Pattern P = Pattern.compile("^#TITANCFG\\{((.+)=(.*))\\}$");

    private static final Logger log =
            LoggerFactory.getLogger(ConfigurationFileFilter.class);

    public static void main(String args[]) throws IOException {

        // Read args[0] as a dirname and iterate recursively over its file contents
        File inputContextDir = new File(args[0]);
        File outputContextDir = new File(args[1]);
        Preconditions.checkArgument(inputContextDir.isDirectory());
        Preconditions.checkArgument(inputContextDir.canRead());

        if (!outputContextDir.exists()) {
            outputContextDir.mkdirs(); // may fail if path exists as a file
        }

        log.info("Input context dir:  {}", inputContextDir);
        log.info("Output context dir: {}", outputContextDir);

        Queue<InputRecord> dirQueue = new LinkedList<InputRecord>();
        dirQueue.add(new InputRecord(inputContextDir, File.separator));
        int visitedDirs = 0;
        int processedFiles = 0;
        InputRecord rec;
        while (null != (rec = dirQueue.poll())) {
            File curDir = rec.getDirectory();
            String contextPath = rec.getContextPath();

            Preconditions.checkState(curDir.exists());
            Preconditions.checkState(curDir.isDirectory());
            Preconditions.checkState(curDir.canRead());

            visitedDirs++;

            for (File f : curDir.listFiles()) {
                if (f.isDirectory()) {
                    if (!f.canRead()) {
                        log.warn("Skipping unreadable directory {} in input basedir", f);
                        continue;
                    }

                    dirQueue.add(new InputRecord(f, contextPath + f.getName() + File.separator));
                } else {
                    if (!f.canRead()) {
                        log.warn("Skipping unreadable file {} in input basedir", f);
                        continue;
                    }

                    File outputDir = new File(outputContextDir.getPath() + contextPath);

                    if (!outputDir.exists()) {
                        outputDir.mkdirs();
                    }

                    processFile(f, new File(outputContextDir.getPath() + contextPath + f.getName()));

                    processedFiles++;
                }
            }
        }

        log.info("Summary: visited {} dirs and processed {} files.", visitedDirs, processedFiles);
    }

    private static class InputRecord {
        private final File directory;
        private final String contextPath;

        private InputRecord(File directory, String relativePath) {
            this.directory = directory;
            this.contextPath = relativePath;
        }

        public File getDirectory() {
            return directory;
        }

        public String getContextPath() {
            return contextPath;
        }
    }

    public static void processFile(File inputFile, File outputFile) throws IOException {
        BufferedReader in = null;
        PrintStream out = null;

        int inputLines = 0;
        int replacements = 0;

        try {
            in = new BufferedReader(new FileReader(inputFile));
            out = new PrintStream(outputFile);

            String line;
            while (null != (line = in.readLine())) {
                inputLines++;
                Matcher m = P.matcher(line);
                if (m.matches()) {
                    String cfgKey = m.group(2).trim();
                    String cfgVal = m.group(3);
                    try {
                        ConfigElement.PathIdentifier pid = ConfigElement.parse(GraphDatabaseConfiguration.ROOT_NS, cfgKey);
                        ConfigOption<?> opt = (ConfigOption<?>) pid.element;
                        String kvPair = m.group(1);
                        String descr = "# "  + WordUtils.wrap(opt.getDescription(), 72, "\n# ", false);
                        String mut = "# Mutability: " + opt.getType();
                        if (opt.isManaged()) {
                            mut += "\n# Use the ManagementSystem to modify this after bootstrapping Titan.";
                        }
                        out.println(descr);
                        out.println(mut);
                        out.println(kvPair);
                        replacements++;
                    } catch (Exception e) {
                        out.println(line);
                        continue;
                    }
                } else {
                    out.println(line);
                }
            }

            log.info("Read {}: {} lines, {} macro substitutions", inputFile, inputLines, replacements);
        } finally {
            IOUtils.closeQuietly(out);
            IOUtils.closeQuietly(in);
        }
    }
}
