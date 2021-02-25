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
import org.janusgraph.graphdb.serializer.attributes.*;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;

@Tag(TestCategory.PERFORMANCE_TESTS)
public class SerializerSpeedTest extends SerializerTestCommon {

    @RepeatedTest(10)
    public void performanceTestStringSerialization() {
        int runs = 100000;
        for (int i = 0; i < runs; i++) {
            multipleStringWrite();
        }
    }

    @RepeatedTest(10)
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
