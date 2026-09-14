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

package org.janusgraph.testutil;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Lets a test observe the log events a class emits.
 * <p>
 * The tests route SLF4J to Log4j 2, so the events of a class (including the events of its {@code private static final}
 * logger field) can be captured by temporarily configuring a Log4j logger for the class name with an in-memory
 * appender. This replaces the former approach of swapping the class' logger field via reflection, which is not
 * possible anymore on Java 12+.
 */
public class TestLoggerUtils {

    private static final Level DEFAULT_LOGGING_LEVEL = Level.DEBUG;

    /**
     * The log events captured for a class while {@link #processWithLogCapture(Class, Level, Consumer)} runs.
     */
    public static class LogCapture {

        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        public List<LogEvent> getEvents() {
            return events;
        }

        public boolean hasEventOfLevel(final Level level) {
            return events.stream().anyMatch(event -> level.equals(event.getLevel()));
        }

        public boolean hasEventOfLevel(final Level level, final String messagePart) {
            return events.stream().anyMatch(event -> level.equals(event.getLevel())
                && event.getMessage().getFormattedMessage().contains(messagePart));
        }
    }

    private static class CapturingAppender extends AbstractAppender {

        private final LogCapture capture;

        private CapturingAppender(final String name, final LogCapture capture) {
            super(name, null, null, true, Property.EMPTY_ARRAY);
            this.capture = capture;
        }

        @Override
        public void append(final LogEvent event) {
            capture.events.add(event.toImmutable());
        }
    }

    public static void processWithLogCapture(final Class<?> classWhoseLogsToCapture,
                                             final Consumer<LogCapture> processWithLogCaptureFunction) {
        processWithLogCapture(classWhoseLogsToCapture, DEFAULT_LOGGING_LEVEL, processWithLogCaptureFunction);
    }

    /**
     * Runs the given function while all log events of the given class at the given level or above are captured.
     * The logger configuration of the class is restored afterwards.
     */
    public static void processWithLogCapture(final Class<?> classWhoseLogsToCapture, final Level loggingLevel,
                                             final Consumer<LogCapture> processWithLogCaptureFunction) {
        final String loggerName = classWhoseLogsToCapture.getName();
        final org.apache.logging.log4j.spi.LoggerContext spiContext = LogManager.getContext(false);
        if (!(spiContext instanceof LoggerContext)) {
            throw new IllegalStateException("Capturing log events requires Log4j Core as the active Log4j implementation but the "
                + "logger context is a " + spiContext.getClass().getName());
        }
        final LoggerContext context = (LoggerContext) spiContext;
        final Configuration configuration = context.getConfiguration();

        // getLoggerConfig() returns the closest parent configuration if the class has no configuration of its own
        final LoggerConfig existingConfig = configuration.getLoggerConfig(loggerName);
        final boolean ownConfig = loggerName.equals(existingConfig.getName());
        final LoggerConfig loggerConfig = ownConfig ? existingConfig : new LoggerConfig(loggerName, loggingLevel, true);
        final Level originalLevel = loggerConfig.getLevel();

        final LogCapture capture = new LogCapture();
        final CapturingAppender appender = new CapturingAppender("TestLogCapture-" + loggerName, capture);
        appender.start();
        if (!ownConfig) {
            configuration.addLogger(loggerName, loggerConfig);
        }
        loggerConfig.addAppender(appender, loggingLevel, null);
        loggerConfig.setLevel(loggingLevel);
        context.updateLoggers();
        try {
            processWithLogCaptureFunction.accept(capture);
        } finally {
            loggerConfig.removeAppender(appender.getName());
            appender.stop();
            if (ownConfig) {
                loggerConfig.setLevel(originalLevel);
            } else {
                configuration.removeLogger(loggerName);
            }
            context.updateLoggers();
        }
    }
}
