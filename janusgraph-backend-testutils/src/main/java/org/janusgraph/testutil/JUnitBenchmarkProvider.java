// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.testutil;

import com.carrotsearch.junitbenchmarks.AutocloseConsumer;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.IResultsConsumer;
import com.carrotsearch.junitbenchmarks.Result;
import com.carrotsearch.junitbenchmarks.WriterConsumer;
import com.carrotsearch.junitbenchmarks.XMLConsumer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

    public static final String ENV_EFFORT_GENERATE = "JUB_EFFORT_GENERATE";
    public static final String ENV_EFFORT_FILE  = "JUB_EFFORT_FILE";
    public static final String ENV_DEFAULT_ROUNDS = "JUB_DEFAULT_ROUNDS";
    public static final String ENV_WARMUP_ROUNDS = "JUB_WARMUP_ROUNDS";
    public static final String ENV_TARGET_RUNTIME_MS = "JUB_TARGET_RUNTIME_MS";

    public static final String DEFAULT_EFFORT_FILE = "../janusgraph-test/data/jub-effort.txt";
    public static final long TARGET_RUNTIME_MS;
    public static final int DEFAULT_ROUNDS;
    public static final int WARMUP_ROUNDS;

    private static final Map<String, Integer> efforts;
    private static final Logger log = LoggerFactory.getLogger(JUnitBenchmarkProvider.class);

    static {
        efforts = loadScalarsFromEnvironment();
        DEFAULT_ROUNDS = loadIntFromEnvironment(ENV_DEFAULT_ROUNDS, 1);
        WARMUP_ROUNDS = loadIntFromEnvironment(ENV_WARMUP_ROUNDS, 1);
        TARGET_RUNTIME_MS = loadIntFromEnvironment(ENV_TARGET_RUNTIME_MS, 5000);
    }

    /**
     * Get a JUnitBenchmarks rule configured for JanusGraph performance testing.
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
    public static TestRule get() {
        return new AdjustableRoundsBenchmarkRule(efforts, getConsumers());
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
    public static TestRule get(IResultsConsumer... additionalConsumers) {
        return new AdjustableRoundsBenchmarkRule(efforts, getConsumers(additionalConsumers));
    }

    /**
     * Get a filename from {@link #ENV_EFFORT_FILE}, then open the file and read
     * method execution multipliers from it. Such a file can be produced using
     * {@link TimeScaleConsumer}.
     *
     * @return map of classname + '.' + methodname to the number of iterations
     *         needed to run for at least {@link #TARGET_RUNTIME_MS}
     */
    private static Map<String, Integer> loadScalarsFromEnvironment() {

        String file = getEffortFilePath();

        File f = new File(file);
        if (!f.canRead()) {
            log.error("Can't read JUnitBenchmarks effort file {}, no effort multipliers loaded.", file);
            return ImmutableMap.of();
        }

        try (final BufferedReader reader = new BufferedReader(new FileReader(file))) {
            return loadScalarsUnsafe(file, reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
        final List<IResultsConsumer> consumers = new ArrayList<>();
        consumers.add(new XMLConsumer(new File("jub." + Math.abs(System.nanoTime()) + ".xml")));
        consumers.add(new WriterConsumer()); // defaults to System.out
        consumers.add(new CsvConsumer("target/jub.csv"));

        if (null != System.getenv(ENV_EFFORT_GENERATE)) {
            String file = getEffortFilePath();
            Writer writer = new FileWriter(file, true);
            log.info("Opened " + file + " for appending");
            consumers.add(new TimeScaleConsumer(writer));
        }

        consumers.addAll(Arrays.asList(additional));

        return consumers.toArray(new IResultsConsumer[consumers.size()]);
    }

    private static String getEffortFilePath() {
        String file = System.getenv(ENV_EFFORT_FILE);
        if (null == file) {
            log.debug("Env variable " + ENV_EFFORT_FILE + " was null");
            log.debug("Defaulting to JUB effort scalar file " + DEFAULT_EFFORT_FILE);
            file = DEFAULT_EFFORT_FILE;
        }
        return file;
    }

    private static Map<String, Integer> loadScalarsUnsafe(String filename, BufferedReader reader) throws IOException {
        String line;
        int ln = 0;
        final int tokensPerLine = 2;
        final ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();

        while (null != (line = reader.readLine())) {
            ln++;
            String[] tokens = line.split(" ");
            if (tokensPerLine != tokens.length) {
                log.warn("Parse error at {}:{}: required {} tokens, but found {} (skipping this line)",
                    filename, ln, tokensPerLine, tokens.length);
                continue;
            }

            int t = 0;
            String name       = tokens[t++];
            String rawscalar  = tokens[t++];

            assert tokensPerLine == t;

            Preconditions.checkNotNull(name);

            if (0 == name.length()) {
                log.warn("Parse error at {}:{}: zero-length method name (skipping this line)", filename, ln);
                continue;
            }

            double scalar;
            try {
                scalar = Double.parseDouble(rawscalar);
            } catch (Exception e) {
                log.warn("Parse error at {}:{}: failed to convert string \"{}\" to a double (skipping this line)",
                    filename, ln, rawscalar);
                log.warn("Double parsing exception stacktrace follows", e);
                continue;
            }

            if (0 > scalar) {
                log.warn("Parse error at {}:{}: read negative method scalar {} (skipping this line)",
                    filename, ln, scalar);
                continue;
            }

            builder.put(name, Double.valueOf(Math.ceil(scalar)).intValue());
        }

        return builder.build();
    }

    /**
     * Write methodnames followed by {@link JUnitBenchmarkProvider#TARGET_RUNTIME_MS} / roundAverage to a file.
     */
    private static class TimeScaleConsumer extends AutocloseConsumer implements Closeable {

        final Writer writer;

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

    private static BenchmarkOptions getDefaultBenchmarkOptions(int rounds) {
        return (BenchmarkOptions)Proxy.newProxyInstance(
                JUnitBenchmarkProvider.class.getClassLoader(), // which classloader is correct?
                new Class[] { BenchmarkOptions.class },
                new DefaultBenchmarkOptionsHandler(rounds));
    }

    private static BenchmarkOptions getWrappedBenchmarkOptions(BenchmarkOptions base, int rounds) {
        return (BenchmarkOptions)Proxy.newProxyInstance(
                JUnitBenchmarkProvider.class.getClassLoader(), // which classloader is correct?
                new Class[] { BenchmarkOptions.class },
                new WrappedBenchmarkOptionsHandler(base, rounds));
    }

    private static int loadIntFromEnvironment(String envKey, int dfl) {
        String s = System.getenv(envKey);

        if (null != s) {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                log.warn("Could not interpret value \"{}\" for environment variable {} as an integer", s, envKey, e);
            }
        } else {
            log.debug("Using default value {} for environment variable {}", dfl, envKey);
        }

        return dfl;
    }


    /**
     * This class uses particularly awkward and inelegant encapsulation. I don't
     * have much flexibility to improve it because both JUnit and
     * JUnitBenchmarks aggressively prohibit inheritance through final and
     * restrictive method/constructor visibility.
     */
    private static class AdjustableRoundsBenchmarkRule implements TestRule {

        private final BenchmarkRule rule;
        private final Map<String, Integer> efforts;

        public AdjustableRoundsBenchmarkRule(Map<String, Integer> efforts, IResultsConsumer... consumers) {
            rule = new BenchmarkRule(consumers);
            this.efforts = efforts;
        }

        @Override
        public Statement apply(Statement base, Description description) {
            Class<?> clazz = description.getTestClass();
            String mname = description.getMethodName();
            Collection<Annotation> annotations = description.getAnnotations();
            final int rounds = getRoundsForFullMethodName(clazz.getCanonicalName() + "." + mname);

            final List<Annotation> modifiedAnnotations = new ArrayList<>(annotations.size());

            boolean hit = false;

            for (Annotation a : annotations) {
                if (a.annotationType().equals(BenchmarkOptions.class)) {
                    final BenchmarkOptions old = (BenchmarkOptions)a;
                    BenchmarkOptions replacement = getWrappedBenchmarkOptions(old, rounds);
                    modifiedAnnotations.add(replacement);
                    log.debug("Modified BenchmarkOptions annotation on {}", mname);
                    hit = true;
                } else {
                    modifiedAnnotations.add(a);
                    log.debug("Kept annotation {} with annotation type {} on {}",
                        a, a.annotationType(), mname);
                }
            }

            if (!hit) {
                BenchmarkOptions opts = getDefaultBenchmarkOptions(rounds);
                modifiedAnnotations.add(opts);
                log.debug("Added BenchmarkOptions {} with annotation type {} to {}",
                    opts, opts.annotationType(), mname);
            }

            Description roundsAdjustedDesc =
                    Description.createTestDescription(
                            clazz, mname,
                            modifiedAnnotations.toArray(new Annotation[modifiedAnnotations.size()]));
            return rule.apply(base, roundsAdjustedDesc);
        }

        private int getRoundsForFullMethodName(String fullname) {
            Integer r = efforts.get(fullname);
            if (null == r) {
                r = DEFAULT_ROUNDS;
                log.warn("Applying default iteration count ({}) to method {}", r, fullname);
            } else {
                log.debug("Loaded iteration count {} on method {}", r, fullname);
            }
            return r;
        }
    }

    private static class DefaultBenchmarkOptionsHandler implements InvocationHandler {

        private final int rounds;

        public DefaultBenchmarkOptionsHandler(int rounds) {
            this.rounds = rounds;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws IllegalArgumentException {
            if (method.getName().equals("benchmarkRounds")) {
                log.trace("Intercepted benchmarkRounds() invocation: returning {}", rounds);
                return rounds;
            }
            if (method.getName().equals("warmupRounds")) {
                log.trace("Intercepted warmupRounds() invocation: returning {}", WARMUP_ROUNDS);
                return WARMUP_ROUNDS;
            }
            if (method.getName().equals("annotationType")) {
                return BenchmarkOptions.class;
            }
            log.trace("Returning default value for method intercepted invocation of method {}", method.getName());
            return method.getDefaultValue();
        }
    }

    private static class WrappedBenchmarkOptionsHandler implements InvocationHandler {

        private final Object base;
        private final int rounds;

        public WrappedBenchmarkOptionsHandler(Object base, int rounds) {
            this.base = base;
            this.rounds = rounds;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws IllegalAccessException, IllegalArgumentException,
                InvocationTargetException {
            if (method.getName().equals("benchmarkRounds")) {
                log.trace("Intercepted benchmarkRounds() invocation: returning {}", rounds);
                return rounds;
            }
            if (method.getName().equals("warmupRounds")) {
                log.trace("Intercepted warmupRounds() invocation: returning {}", WARMUP_ROUNDS);
                return WARMUP_ROUNDS;
            }
            log.trace("Delegating intercepted invocation of method {} to wrapped base instance {}", method.getName(), base);
            return method.invoke(base, args);
        }

    }
}
