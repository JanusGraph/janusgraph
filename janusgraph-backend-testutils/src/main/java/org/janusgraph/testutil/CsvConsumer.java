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

import com.carrotsearch.junitbenchmarks.IResultsConsumer;
import com.carrotsearch.junitbenchmarks.Result;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class CsvConsumer implements IResultsConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(CsvConsumer.class);

    private final Writer csv;

    private final File csvFile;

    private final String separator = ",";

    public enum Column {
        CLASS_NAME("class.name") {
            @Override public String get(Result r) {
                return r.getShortTestClassName();
            }
        },
        METHOD_NAME("method.name") {
            @Override public String get(Result r) {
                return r.getTestMethodName();
            }
        },
        ROUND_COUNT("round.measured") {
            @Override
            public String get(Result r) {
                return String.valueOf(r.benchmarkRounds);
            }
        },
        ROUND_WARMUP("round.warmup") {
            @Override public String get(Result r) {
                return String.valueOf(r.warmupRounds);
            }
        },
        // Called "round" in JUB's standard WriterConsumer,
        // but that's ambiguous with round counts above
        ROUND_AVG("round.time") {
            @Override public String get(Result r) {
                return String.valueOf(r.roundAverage.avg); // ms
            }
        },
        ROUND_AVG_STDEV("round.time.stdev") {
            @Override public String get(Result r) {
                return String.valueOf(r.roundAverage.stddev);
            }
        },
        ROUND_BLOCK("round.block") {
            @Override public String get(Result r) {
                return String.valueOf(r.blockedAverage.avg); // ms
            }
        },
        ROUND_BLOCK_STDEV("round.block.stdev") {
            @Override public String get(Result r) {
                return String.valueOf(r.roundAverage.stddev);
            }
        },
        ROUND_GC("round.gc") {
            @Override public String get(Result r) {
                return String.valueOf(r.gcAverage.avg); // ms
            }
        },
        ROUND_GC_STDEV("round.gc.stdev") {
            @Override public String get(Result r) {
                return String.valueOf(r.gcAverage.stddev);
            }
        },
        GC_CALLS("gc.calls") {
            @Override public String get(Result r) {
                return String.valueOf(r.gcInfo.accumulatedInvocations());
            }
        },
        GC_TIME("gc.time") {
            @Override public String get(Result r) {
                return String.valueOf(r.gcInfo.accumulatedTime() / 1000); // ms
            }
        },
        TIME_TOTAL("time.total") {
            @Override public String get(Result r) {
                return String.valueOf((r.benchmarkTime + r.warmupTime) / 1000); // ms
            }
        },
        TIME_WARMUP("time.warmup") {
            @Override public String get(Result r) {
                return String.valueOf(r.warmupTime / 1000); // ms
            }
        },
        TIME_BENCH("time.bench") {
            @Override public String get(Result r) {
                return String.valueOf(r.benchmarkTime / 1000); // ms
            }
        };

        public abstract String get(Result r);

        private final String name;

        Column(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public CsvConsumer(String fileName) throws IOException {
        csvFile = new File(fileName);
        log.debug("Opening {} in append mode", csvFile);
        csv = new OutputStreamWriter(new FileOutputStream(csvFile, true));
        printHeader();
    }

    public synchronized void accept(Result r) throws IOException {
        Joiner j = Joiner.on(separator);
        final List<String> fields = new ArrayList<>(Column.values().length);
        for (Column c : Column.values()) {
            fields.add(c.get(r));
        }
        csv.write(String.format("%s%n", j.join(fields)));
        log.debug("Wrote {} to {}", r, csvFile);
        csv.flush();
    }

    private synchronized void printHeader() throws IOException {
        long len = csvFile.length();
        if (0 != len) {
            log.debug("Not writing header to {}; file has non-zero length {}", csvFile, len);
            return;
        }

        Joiner j = Joiner.on(separator);
        final List<String> headers = new ArrayList<>(Column.values().length);
        for (Column c : Column.values()) {
            headers.add(c.getName());
        }
        csv.write(String.format("%s%n", j.join(headers)));
        log.debug("Wrote header to {}", csvFile);
        csv.flush();
    }
}
