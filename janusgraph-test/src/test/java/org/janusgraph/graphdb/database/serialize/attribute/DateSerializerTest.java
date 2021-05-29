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

import org.apache.tinkerpop.shaded.jackson.databind.util.StdDateFormat;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DateSerializerTest {
    private final StandardSerializer serializer = new StandardSerializer();

    private static Stream<Arguments> params() {
        return Stream.of(
            Arguments.of(new Date(123), 123L),
            Arguments.of(new Date(123), "123"),
            Arguments.of(new Date(1557532800000L), "2019-05-11"),
            Arguments.of(new Date(1557602288000L), "2019-05-11T19:18:08+00:00"),
            Arguments.of(new Date(1557602288000L), "2019-05-11T19:18:08Z"),
            Arguments.of(new Date(1557602288000L), "2019-05-11T19:18:08"),
            Arguments.of(new Date(1557598680000L), "2019-05-11T19:18+01:00"),
            Arguments.of(new Date(1557602280000L), "2019-05-11T19:18Z"),
            Arguments.of(new Date(1557602280000L), "2019-05-11T19:18"),
            Arguments.of(new Date(1557602288200L), "2019-05-11T19:18:08.2"),
            Arguments.of(new Date(1557566288400L), "2019-05-11T19:18:08.400+10"),
            Arguments.of(new Date(1557613088010L), "2019-05-11T19:18:08.01-0300"),
            Arguments.of(new Date(1557602280000L), "2019-05-11T19:18Z"),
            Arguments.of(new Date(1557602280000L), "2019-05-11T19:18"),
            Arguments.of(new Date(786297600000L), "Thu, 01 Dec 1994 16:00:00 GMT"),
            Arguments.of(new Date(784111777000L), "Sun, 06 Nov 1994 08:49:37 GMT")
        );
    }

    @ParameterizedTest
    @MethodSource("params")
    public void dateSerializerConvertString(Date expected, Object value) {
        Date actual = serializer.convert(Date.class, value);
        assertEquals(expected, actual);
    }

    @Test
    public void dateSerializerConvertStringThreadSafe() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(64);
        String input = "2021-01-30T17:30:31.000";
        Date reference = StdDateFormat.instance.parse(input);

        List<Future<Date>> futures = new ArrayList<>();
        try {
            // Have serializer parse the same date 100x in parallel
            for (int i = 0; i < 100; ++i) {
                futures.add(pool.submit(() -> serializer.convert(Date.class, input)));
            }
            for (Future<Date> future : futures) {
                assertEquals(reference, future.get());
            }
        } finally {
            pool.shutdown();
        }
    }
}
