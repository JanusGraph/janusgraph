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

package org.janusgraph.graphdb;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.LogManager;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.diskstorage.log.util.FutureMessage;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_NS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TestMockLog implements LogManager {

    public static final ConfigOption<Boolean> LOG_MOCK_FAILADD = new ConfigOption<>(LOG_NS, "fail-adds",
            "Sets the log to reject adding messages. FOR TESTING ONLY",
            ConfigOption.Type.LOCAL, false).hide();

    private final Map<String,TestLog> openLogs = Maps.newHashMap();
    private final boolean failAdds;
    private final String senderId;
    private final TimestampProvider times;

    public TestMockLog(Configuration config) {
        this.failAdds = config.get(LOG_MOCK_FAILADD);
        this.senderId = config.get(UNIQUE_INSTANCE_ID);
        this.times = config.get(TIMESTAMP_PROVIDER);
    }

    @Override
    public synchronized Log openLog(String name) {
        return openLogs.computeIfAbsent(name, TestLog::new);
    }

    @Override
    public synchronized void close() {
        openLogs.clear();
    }


    private class TestLog implements Log {

        private final String name;
        private final Set<MessageReader> readers =Sets.newHashSet();
        private List<FutureMessage<TestMessage>> messageBacklog = Lists.newArrayList();

        private TestLog(String name) {
            this.name = name;
        }

        @Override
        public synchronized Future<Message> add(StaticBuffer content) {
            final TestMessage msg = new TestMessage(content);
            final FutureMessage<TestMessage> fmsg = new FutureMessage<>(msg);

            if (failAdds) {
                System.out.println("Failed message add");
                throw new JanusGraphException("Log unavailable");
            }

            if (readers.isEmpty()) {
                messageBacklog.add(fmsg);
            } else {
                process(fmsg);
            }
            return fmsg;
        }

        private void process(FutureMessage<TestMessage> fmsg) {
            for (MessageReader reader : readers) {
                reader.read(fmsg.getMessage());
            }
            fmsg.delivered();
        }

        @Override
        public synchronized Future<Message> add(StaticBuffer content, StaticBuffer key) {
            return add(content);
        }

        @Override
        public synchronized void registerReader(ReadMarker readMarker, MessageReader... reader) {
            registerReaders(readMarker, Arrays.asList(reader));
        }

        @Override
        public synchronized void registerReaders(ReadMarker readMarker, Iterable<MessageReader> readers) {
            for (FutureMessage<TestMessage> fmsg : messageBacklog) {
                process(fmsg);
            }
            messageBacklog=null;
            Iterables.addAll(this.readers,readers);
        }

        @Override
        public synchronized boolean unregisterReader(MessageReader reader) {
            return readers.remove(reader);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void close() {
            readers.clear();
        }
    }

    private class TestMessage implements Message {

        private final Instant time;
        private final StaticBuffer content;

        private TestMessage(StaticBuffer content) {
            this.time = times.getTime();
            this.content = content;
        }

        @Override
        public String getSenderId() {
            return senderId;
        }

        @Override
        public Instant getTimestamp() {
            return time;
        }

        @Override
        public StaticBuffer getContent() {
            return content;
        }
    }


}
