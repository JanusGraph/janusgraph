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

package org.janusgraph.graphdb.serializer;

import org.janusgraph.TestCategory;
import org.janusgraph.graphdb.serializer.attributes.TClass1;
import org.janusgraph.graphdb.serializer.attributes.TClass1Serializer;
import org.janusgraph.graphdb.serializer.attributes.TClass2;
import org.janusgraph.graphdb.serializer.attributes.TClass2Serializer;
import org.janusgraph.graphdb.serializer.attributes.TEnum;
import org.janusgraph.graphdb.serializer.attributes.TEnumSerializer;
import org.janusgraph.testutil.JUnitBenchmarkProvider;
import org.junit.Rule;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.rules.TestRule;

@Tag(TestCategory.PERFORMANCE_TESTS)
public class SerializerSpeedTest extends SerializerTestCommon {

    @Rule
    public TestRule benchmark = JUnitBenchmarkProvider.get();

    @Test
    public void performanceTestStringSerialization() {
        int runs = 100000;
        for (int i = 0; i < runs; i++) {
            multipleStringWrite();
        }
    }

    @Test
    public void performanceTestObjectSerialization() {
        serialize.registerClass(2,TClass1.class, new TClass1Serializer());
        serialize.registerClass(80342,TClass2.class, new TClass2Serializer());
        serialize.registerClass(999,TEnum.class, new TEnumSerializer());
        int runs = 1000000;
        for (int i = 0; i < runs; i++) {
            objectWriteRead();
        }
    }
}
