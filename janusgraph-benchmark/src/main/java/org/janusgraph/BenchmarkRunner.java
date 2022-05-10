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

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

public class BenchmarkRunner {
    public static void main(String[] args) throws RunnerException {
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
        new Runner(builder.build()).run();
    }
}
