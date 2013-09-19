package com.thinkaurelius.titan.testutil;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.AutocloseConsumer;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.IResultsConsumer;
import com.carrotsearch.junitbenchmarks.Result;
import com.carrotsearch.junitbenchmarks.WriterConsumer;
import com.carrotsearch.junitbenchmarks.XMLConsumer;
import com.google.common.collect.ImmutableMap;

/**
 * JUB can write the results of a single JVM run to an XML file, but it does not
 * support appending to an existing file. When given a path to an existing file,
 * it silently overwrites the old contents. This is unusable in a
 * fork-per-test-class surefire configuration. The results of each test class
 * overwrite those previous, so that the only results still readable at the end
 * of a test run are those of the final class executed.
 * <p>
 * This class exists to configure JUB programmatically and avoid the annoying
 * behavior of the system-property-configured XMLConsumer.
 */
public class JUnitBenchmarkProvider {
    
    public static final String ENV_WRITE_FILE = "JUB_WRITE_SCALARS";
    public static final String ENV_READ_FILE  = "JUB_READ_SCALARS";
    public static final long TARGET_RUNTIME_MS = 1000L;
    
    private static final Logger log = LoggerFactory.getLogger(JUnitBenchmarkProvider.class);
    
    /**
     * Get a JUnitBenchmarks rule configured for Titan performance testing.
     * <p>
     * The returned rule will write results to an XML file named
     * jub.(abs(current nanotime)).xml and to the console.
     * <p>
     * This method concentrates our JUB configuration in a single code block and
     * gives us programmatic flexibility that exceeds the limited flexibility of
     * configuring JUB through its hardcoded global system properties. It also
     * converts the IOException that XMLConsumer's constructor can throw into a
     * RuntimeException. In test classes, this conversion is the difference
     * between:
     * 
     * <pre>
     * {@literal @}Rule
     * public TestRule benchmark; // Can't initialize here b/c of IOException
     * ...
     * public TestClassConstructor() throws IOException {
     *     benchmark = new BenchmarkRule(new XMLConsumer(...));
     * }
     * 
     * // or, if there are extant subclass constructors we want to leave alone...
     * 
     * public TestClassConstructor() {
     *     try {
     *         benchmark = new BenchmarkRule(new XMLConsumer(...));
     *     } catch (IOException e) {
     *         throw new RuntimeException(e);
     *     }
     * }
     * </pre>
     * 
     * versus, with this method,
     * 
     * <pre>
     * {@literal @}Rule
     * public TestRule benchmark = JUnitBenchmarkProvider.get(); // done
     * </pre>
     * 
     * @return a BenchmarkRule ready for use with the JUnit @Rule annotation
     */
    public static BenchmarkRule get() {
        return new BenchmarkRule(getConsumers());
    }
    
    /**
     * Like {@link #get()}, except extra JUB Results consumers can be attached
     * to the returned rule.
     * 
     * @param additionalConsumers
     *            extra JUB results consumers to apply in the returned rule
     *            object
     * @return a BenchmarkRule ready for use with the JUnit @Rule annotation
     */
    public static BenchmarkRule get(IResultsConsumer... additionalConsumers) {
        return new BenchmarkRule(getConsumers(additionalConsumers));
    }
    
    /**
     * Get a filename from {@link #ENV_READ_FILE}, then open the file and read
     * method execution multipliers from it. Such a file can be produced using
     * {@link TimeScaleConsumer}.
     * 
     * @return map of classname + '.' + methodname to the number of iterations
     *         needed to run for at least {@link #TARGET_RUNTIME_MS}
     */
    public static Map<String, Double> loadScalarsFromEnvironment() {

        String filename = System.getenv(ENV_READ_FILE);
        
        if (null == filename) {
            return ImmutableMap.of();
        }
        
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filename));
            return loadScalarsUnsafe(filename, reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (null != reader) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static IResultsConsumer[] getConsumers(IResultsConsumer... additional) {
        try {
            return getConsumersUnsafe(additional);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static IResultsConsumer[] getConsumersUnsafe(IResultsConsumer... additional) throws IOException {
        List<IResultsConsumer> consumers = new ArrayList<IResultsConsumer>();
        consumers.add(new XMLConsumer(new File("jub." + Math.abs(System.nanoTime()) + ".xml")));
        consumers.add(new WriterConsumer());
        String file = System.getenv(ENV_WRITE_FILE);
        if (null != file) {
            Writer writer = new FileWriter(file, true);
            consumers.add(new TimeScaleConsumer(writer));
            log.info("Opened " + file + " for appending");
        } else {
            log.debug("Env variable " + ENV_WRITE_FILE + " was null");
        }
        
        return consumers.toArray(new IResultsConsumer[consumers.size()]);
    }
    
    private static Map<String, Double> loadScalarsUnsafe(String filename, BufferedReader reader) throws IOException {
        String line;
        int ln = 0;
        final int tokensPerLine = 2;
        final ImmutableMap.Builder<String, Double> builder = new ImmutableMap.Builder<String, Double>();
        
        while (null != (line = reader.readLine())) {
            ln++;
            String[] tokens = line.split(" ");
            if (tokensPerLine != tokens.length) {
                log.warn("Parse error at {}:{}: required {} tokens, but found {} (skipping this line)", 
                        new Object[] { filename, ln, tokensPerLine, tokens.length });
                continue;
            }
            
            int t = 0;
            String name       = tokens[t++];
            String rawscalar  = tokens[t++];
            assert tokensPerLine == t;
            
            assert null != name;
            
            if (0 == name.length()) {
                log.warn("Parse error at {}:{}: zero-length method name (skipping this line)", filename, ln);
                continue;
            }
            
            assert 0 < name.length();
            
            Double scalar;
            try {
                scalar = Double.valueOf(rawscalar);
            } catch (Exception e) {
                log.warn("Parse error at {}:{}: failed to convert string \"{}\" to a double (skipping this line)", 
                        new Object[] { filename, ln, rawscalar });
                log.warn("Double parsing exception stacktrace follows", e);
                continue;
            }
            
            if (0 > scalar) {
                log.warn("Parse error at {}:{}: read negative method scalar {} (skipping this line)",
                        new Object[] { filename, ln, scalar });
                continue;
            }
            
            assert null != scalar;
            
            builder.put(name, scalar);
        }
        
        return builder.build();
    }
    
    /**
     * Write methodnames followed by 1000 / roundAverage to a file.
     */
    public static class TimeScaleConsumer extends AutocloseConsumer implements Closeable {
        
        Writer writer;
        
        public TimeScaleConsumer(Writer writer) {
            this.writer = writer;
        }

        @Override
        public void accept(Result result) throws IOException {
            
            // Result's javadoc says roundAvg.avg is ms, but it seems to be s in reality
            double millis = 1000D * result.roundAverage.avg;
            double scalar = Math.max(1D, TARGET_RUNTIME_MS / Math.max(1D, millis));
            
            String testClass = result.getTestClassName();
            String testName = result.getTestMethodName();
            writer.write(String.format("%s.%s %.3f%n", testClass, testName, scalar));
            writer.flush();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}
