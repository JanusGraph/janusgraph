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

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.util.ByteBufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(jvmArgsAppend = "-Xmx1G")
public class StaticArrayEntryListBenchmark {
    List<Entry> entries = new ArrayList<>();

    @Param({ "10000", "100000" })
    Integer size;

    @Param({ "50", "1000", "5000" })
    Integer valueSize;

    @Setup
    public void setup() {
        for (int i = 0; i < size; i++) {
            StaticArrayBuffer column = StaticArrayEntry.of(ByteBufferUtil.oneByteBuffer(20));
            StaticArrayBuffer value = StaticArrayEntry.of(ByteBufferUtil.oneByteBuffer(valueSize));
            Entry entry = StaticArrayEntry.of(column, value);
            entries.add(entry);
        }
    }

    @Benchmark
    public void iterator(Blackhole bh) {
        EntryList result = StaticArrayEntryList.ofStaticBuffer(entries.iterator(), StaticArrayEntry.ENTRY_GETTER);
        bh.consume(result);
    }

    @Benchmark
    public void iterable(Blackhole bh) {
        EntryList result = StaticArrayEntryList.ofStaticBuffer(entries, StaticArrayEntry.ENTRY_GETTER);
        bh.consume(result);
    }
}
