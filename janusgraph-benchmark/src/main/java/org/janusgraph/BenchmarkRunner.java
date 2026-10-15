// Copyright 2021 JanusGraph Authors
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

package org.janusgraph;

import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchmarkRunner {
    // The milliseconds in each unit of time per operation which JMH reports, as the results are all given in ms/op,
    // whatever unit a benchmark measures in
    private static final Map<String, Double> MILLISECONDS_PER_UNIT = Map.of(
        "ns/op", 1e-6, "us/op", 1e-3, "ms/op", 1.0, "s/op", 1e3, "min/op", 6e4, "hr/op", 3.6e6, "day/op", 8.64e7);

    private static void transformResults(Collection<RunResult> results, List<Map<String, Object>> outputs) {
        Map<String, Double> metrics = new HashMap<>();
        for (RunResult result : results) {
            Result primaryResult = result.getPrimaryResult();
            String benchmark = result.getParams().getBenchmark();
            double score = primaryResult.getScore();
            String unit = primaryResult.getScoreUnit();
            Double millisecondsPerUnit = MILLISECONDS_PER_UNIT.get(unit);
            if (millisecondsPerUnit == null) {
                throw new IllegalArgumentException("Please measure the time per operation of benchmark " + benchmark
                    + ", not " + unit);
            }
            // we sum up results with different params against same benchmark
            metrics.put(benchmark, metrics.getOrDefault(benchmark, 0d) + score * millisecondsPerUnit);
        }
        for (Map.Entry<String, Double> entry : metrics.entrySet()) {
            outputs.add(new HashMap() {{
                put("name", entry.getKey());
                put("value", entry.getValue());
                put("unit", "ms/op");
            }});
        }
    }

    private static void runCqlBenchmarks(ChainedOptionsBuilder cqlBuilder, List<Map<String, Object>> outputs) throws RunnerException, InterruptedException, IOException {
        CcmBridge.Builder builder =
            CcmBridge.builder()
                .withNodes(1);
        CcmBridge bridge = builder.build();
        Runtime.getRuntime().addShutdownHook(
            new Thread(
                () -> {
                    bridge.stop();
                    bridge.remove();
                }
            )
        );
        bridge.create();
        bridge.start();
        System.out.println("Cassandra version = " + bridge.getCassandraVersion());

        cqlBuilder.include("CQL.*Benchmark");
        // CQLMultiQueryHasStepBenchmark is a long-running benchmark. Explicitly run this benchmark if necessary.
        cqlBuilder.exclude(CQLMultiQueryHasStepBenchmark.class.getSimpleName());
        // CQLMultiQueryPropertiesBenchmark is a long-running benchmark. Explicitly run this benchmark if necessary.
        cqlBuilder.exclude(CQLMultiQueryPropertiesBenchmark.class.getSimpleName());

        transformResults(new Runner(cqlBuilder.build()).run(), outputs);
    }

    private static ChainedOptionsBuilder getJmhBuilder() {
        return new OptionsBuilder()
            .forks(1)
            .measurementTime(TimeValue.seconds(5))
            .shouldFailOnError(true)
            .warmupIterations(2)
            .warmupTime(TimeValue.seconds(1));
    }

    public static void main(String[] args) throws RunnerException, IOException, InterruptedException {
        final boolean runSpecifiedTests = args.length > 0;
        final List<Map<String, Object>> outputs = new ArrayList<>();

        final ChainedOptionsBuilder builder = getJmhBuilder();
        if (runSpecifiedTests) {
            for (String arg : args) {
                builder.include(arg);
            }
        } else {
            builder.include(".*Benchmark");
            builder.exclude(StaticArrayEntryListBenchmark.class.getSimpleName());
            builder.exclude(BackPressureBenchmark.class.getSimpleName());
            // These need an Elasticsearch server, which this run doesn't start: IndexQueryResultBenchmark and
            // TransactionGetOrCreateBenchmark for their mixed index, and TransactionChangedVerticesBenchmark always
            builder.exclude(IndexQueryResultBenchmark.class.getSimpleName());
            builder.exclude(TransactionGetOrCreateBenchmark.class.getSimpleName());
            builder.exclude(TransactionChangedVerticesBenchmark.class.getSimpleName());
            // Its commits which BerkeleyJE syncs to disk mostly measure the disk of the machine, and its 10 cases would
            // take some five minutes; run it explicitly
            builder.exclude(BerkeleyJELockBenchmark.class.getSimpleName());
            builder.exclude("CQL.*Benchmark");
            builder.exclude(".*ElasticSearch.*Benchmark.*");
        }
        Collection<RunResult> results = new Runner(builder.build()).run();
        transformResults(results, outputs);

        // run benchmarks using Cassandra (if ccm is available)
        if (!runSpecifiedTests) {
            runCqlBenchmarks(getJmhBuilder(), outputs);
        }

        File file = new File("benchmark.json");
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(outputs);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(json);
        writer.close();
    }
}
