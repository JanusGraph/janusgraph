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
    private static void transformResults(Collection<RunResult> results, List<Map<String, Object>> outputs) {
        Map<String, Double> metrics = new HashMap<>();
        for (RunResult result : results) {
            Result primaryResult = result.getPrimaryResult();
            String benchmark = result.getParams().getBenchmark();
            double score = primaryResult.getScore();
            String unit = primaryResult.getScoreUnit();
            if (!"ms/op".equals(unit)) {
                throw new IllegalArgumentException("Please use ms/op as measurement for benchmark " + benchmark);
            }
            // we sum up results with different params against same benchmark
            metrics.put(benchmark, metrics.getOrDefault(benchmark, 0d) + score);
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
            builder.exclude("StaticArrayEntryListBenchmark");
            builder.exclude("VertexCacheBenchmark");
            builder.exclude("BackPressureBenchmark");
            builder.exclude("CQL.*Benchmark");
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
