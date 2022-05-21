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
    public static void main(String[] args) throws RunnerException, IOException {
        final ChainedOptionsBuilder builder = new OptionsBuilder()
            .forks(1)
            .measurementTime(TimeValue.seconds(5))
            .shouldFailOnError(true)
            .warmupIterations(2)
            .warmupTime(TimeValue.seconds(1));
        if (args.length > 0) {
            for (String arg : args) {
                builder.include(arg);
            }
        } else {
            builder.include(".*Benchmark");
        }
        builder.exclude(".*StaticArrayEntryListBenchmark");
        Collection<RunResult> results = new Runner(builder.build()).run();
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
        List<Map<String, Object>> outputs = new ArrayList<>();
        for (Map.Entry<String, Double> entry : metrics.entrySet()) {
            outputs.add(new HashMap() {{
                put("name", entry.getKey());
                put("value", entry.getValue());
                put("unit", "ms/op");
            }});
        }
        String workspace = System.getenv("${GITHUB_WORKSPACE}");
        File file = new File(workspace, "benchmark.json");
        System.out.println("writing to " + file.getAbsolutePath());
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(outputs);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(json);
        writer.close();
    }
}
