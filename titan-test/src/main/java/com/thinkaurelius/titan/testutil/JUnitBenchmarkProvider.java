package com.thinkaurelius.titan.testutil;

import java.io.File;
import java.io.IOException;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.WriterConsumer;
import com.carrotsearch.junitbenchmarks.XMLConsumer;

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
        try {
            return new BenchmarkRule(
                    new XMLConsumer(new File("jub." + Math.abs(System.nanoTime()) + ".xml")),
                    new WriterConsumer());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
