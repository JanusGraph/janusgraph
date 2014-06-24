package com.thinkaurelius.titan.graphdb.serializer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

import com.thinkaurelius.titan.testcategory.PerformanceTests;
import com.thinkaurelius.titan.testutil.JUnitBenchmarkProvider;

@Category({ PerformanceTests.class })
public class SerializerSpeedTest extends SerializerTestCommon {

    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get();

    @Test
    public void performanceTestLong() {
        int runs = 1000;
        for (int i = 0; i < runs; i++) {
            longWrite();
        }
    }

    @Test
    public void performanceTestShort() {
        int runs = 1000;
        for (int i = 0; i < runs; i++) {
            objectWriteRead();
        }
    }
}
