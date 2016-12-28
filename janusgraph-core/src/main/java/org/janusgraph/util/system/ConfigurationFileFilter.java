package org.janusgraph.util.system;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.lang.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigurationFileFilter {

    private static final Pattern REPLACEMENT_PATTERN = Pattern.compile("^#TITANCFG\\{((.+)=(.*))\\}$");

    private static final Logger log =
            LoggerFactory.getLogger(ConfigurationFileFilter.class);

    private static final int WRAP_COLUMNS = 72;

    public static void main(String args[]) throws IOException {
        int errors = filter(args[0], args[1]);
        System.exit(Math.min(errors, 127));
    }

    public static int filter(String inputContextDirPath, String outputContextDirPath) throws IOException {

        // Read args[0] as a dirname and iterate recursively over its file contents
        File inputContextDir = new File(inputContextDirPath);
        File outputContextDir = new File(outputContextDirPath);

        log.info("Input context dir:  {}", inputContextDir);
        log.info("Output context dir: {}", outputContextDir);
        Preconditions.checkArgument(inputContextDir.isDirectory(),
                "Input context dir %s is not a directory", inputContextDir);
        Preconditions.checkArgument(inputContextDir.canRead(),
                "Input context dir %s is not readable", inputContextDir);

        if (!outputContextDir.exists()) {
            outputContextDir.mkdirs(); // may fail if path exists as a file
        }

        Queue<InputRecord> dirQueue = new LinkedList<InputRecord>();
        dirQueue.add(new InputRecord(inputContextDir, File.separator));
        int parseErrors = 0;
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

                    parseErrors += processFile(f, new File(outputContextDir.getPath() + contextPath + f.getName()));

                    processedFiles++;
                }
            }
        }

        String summaryTemplate = "Summary: visited {} dir(s) and processed {} file(s) with {} parse error(s).";

        if (0 == parseErrors) {
            log.info(summaryTemplate, visitedDirs, processedFiles, parseErrors);
        } else {
            log.error(summaryTemplate, visitedDirs, processedFiles, parseErrors);
        }

        return parseErrors;
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

    private static int processFile(File inputFile, File outputFile) throws IOException {
        BufferedReader in = null;
        PrintStream out = null;

        int inputLines = 0;
        int replacements = 0;
        int parseErrors = 0;

        try {
            in = new BufferedReader(new FileReader(inputFile));
            out = new PrintStream(outputFile);

            String line;

            while (null != (line = in.readLine())) {
                inputLines++;
                Matcher m = REPLACEMENT_PATTERN.matcher(line);
                if (m.matches()) {
                    String cfgKey = m.group(2).trim();
                    String cfgVal = m.group(3);
                    try {
                        ConfigElement.PathIdentifier pid = ConfigElement.parse(GraphDatabaseConfiguration.ROOT_NS, cfgKey);
                        ConfigOption<?> opt = (ConfigOption<?>) pid.element;
                        //opt.verify(cfgVal);
                        String kvPair = m.group(1);
                        String descr = "# " + WordUtils.wrap(opt.getDescription(), WRAP_COLUMNS, "\n# ", false);
                        String dt = "# Data Type:  ";
                        if (opt.getDatatype().isArray()) {
                            dt += opt.getDatatype().getComponentType().toString() + "[]";
                        } else if (opt.getDatatype().isEnum()) {
                            Enum[] enums = (Enum[])opt.getDatatype().getEnumConstants();
                            String[] names = new String[enums.length];
                            for (int i = 0; i < names.length; i++)
                                names[i] = enums[i].name();
                            dt += opt.getDatatype().getSimpleName() + " enum:";
                            String s = "\n#             " + "{ " + Joiner.on(", ").join(names) + " }";
                            dt += WordUtils.wrap(s, WRAP_COLUMNS, "\n#               ", false);
                        } else {
                            dt += opt.getDatatype().getSimpleName();
                        }
                        String defval = "# Default:    ";
                        if (null == opt.getDefaultValue()) {
                            defval += "(no default value)";
                        } else if (opt.getDatatype().isArray()) {
                            defval += Joiner.on(", ").join((Object[]) opt.getDefaultValue());
                        } else if (opt.getDatatype().isEnum()) {
                            defval += ((Enum)opt.getDefaultValue()).name();
                        } else {
                            defval += opt.getDefaultValue();
                        }
                        String mut = "# Mutability: " + opt.getType();
                        if (opt.isManaged()) {
                            mut += "\n#\n# ";
                            if (opt.getType().equals(ConfigOption.Type.FIXED)) {
                                mut += "This setting is " + opt.getType() +
                                        " and cannot be changed after bootstrapping Titan.";
                            } else {
                                final String warning =
                                    "Settings with mutability " + opt.getType() + " are centrally managed in " +
                                    "Titan's storage backend.  After starting the database for the first time, " +
                                    "this file's copy of this setting is ignored.  Use Titan's Management " +
                                    "System to read or modify this value after bootstrapping.";
                                mut += WordUtils.wrap(warning, WRAP_COLUMNS, "\n# ", false);
                            }
                        }
                        String umbrella = null;
                        // This background info on umbrellas isn't critical when getting started with an Titan config
//                        if (pid.hasUmbrellaElements() && 1 == pid.umbrellaElements.length) {
//                            String s = "# " +
//                                    "This setting is under an umbrella namespace.  " +
//                                    "The string \"" + pid.umbrellaElements[0] + "\" in this setting groups " +
//                                    "it with other elements under the same umbrella. " +
//                                    "See the Configuration Reference in the manual for more info.";
//                            umbrella = WordUtils.wrap(s, WRAP_COLUMNS, "\n# ", false);
//                        }

                        out.println(descr);
                        out.println("#");
                        out.println(defval);
                        out.println(dt);
                        out.println(mut);
                        if (null != umbrella)
                            out.println(umbrella);
                        out.println(kvPair);
                        replacements++;
                    } catch (RuntimeException e) {
                        out.println(line);

                        log.warn("Exception on {}:{}", inputFile, line, e);
                        parseErrors++;
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

        // Read what we just wrote.  Make sure it validates as a Titan config.
        ConfigurationLint.Status stat = ConfigurationLint.validate(outputFile.getAbsolutePath());
        if (0 != stat.getErrorSettingCount())
            log.error("Output file {} failed to validate", outputFile);

        parseErrors += stat.getErrorSettingCount();

        return parseErrors;
    }
}
