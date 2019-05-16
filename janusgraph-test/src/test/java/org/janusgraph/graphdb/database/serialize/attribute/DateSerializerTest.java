// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.database.serialize.attribute;

import org.janusgraph.graphdb.database.serialize.StandardSerializer;

import java.util.Date;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DateSerializerTest {
    private StandardSerializer serializer = new StandardSerializer();

    private static Stream<Arguments> params() {
        return Stream.of(
            Arguments.of(new Date(123), 123L),
            Arguments.of(new Date(123), "123"),
            Arguments.of(new Date(1557532800000L), "2019-05-11"),
            Arguments.of(new Date(1557602288000L), "2019-05-11T19:18:08+00:00"),
            Arguments.of(new Date(1557602288000L), "2019-05-11T19:18:08Z"),
            Arguments.of(new Date(786297600000L), "Thu, 01 Dec 1994 16:00:00 GMT"),
            Arguments.of(new Date(784111777000L), "Sun, 06 Nov 1994 08:49:37 GMT")
        );
    }

    @ParameterizedTest
    @MethodSource("params")
    public void dateSerializerConvertString(Date expected, Object value) {
        Date actual = serializer.convert(Date.class, value);
        Assert.assertEquals(expected, actual);
    }
}
