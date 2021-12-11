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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.function.Consumer;

public class TestLoggerUtils {

    private static final Constructor<ch.qos.logback.classic.Logger> LOGBACK_CONSTRUCTOR;
    private static final LoggerContext LOGBACK_CONTEXT = new LoggerContext();
    private static final ch.qos.logback.classic.Logger ROOT_LOGGER = LOGBACK_CONTEXT.getLogger("ROOT");
    private static final Level DEFAULT_LOGGING_LEVEL = Level.DEBUG;

    static {
        try {
            LOGBACK_CONSTRUCTOR = ch.qos.logback.classic.Logger.class.getDeclaredConstructor(
                String.class, ch.qos.logback.classic.Logger.class, LoggerContext.class);
            LOGBACK_CONSTRUCTOR.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    public static ch.qos.logback.classic.Logger createLogbackLogger(Class<?> clazz, Level loggingLevel){
        try {
            ch.qos.logback.classic.Logger logger = LOGBACK_CONSTRUCTOR.newInstance(clazz.getName(), ROOT_LOGGER, LOGBACK_CONTEXT);
            logger.setLevel(loggingLevel);
            return logger;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void processWithLoggerReplacement(Consumer<ch.qos.logback.classic.Logger> processWithLoggerReplacementFunction,
                                                    Class<?> classWhereToReplaceLogger){
        processWithLoggerReplacement(processWithLoggerReplacementFunction, classWhereToReplaceLogger, DEFAULT_LOGGING_LEVEL);
    }

    public static void processWithLoggerReplacement(Consumer<ch.qos.logback.classic.Logger> processWithLoggerReplacementFunction,
                                                    Class<?> classWhereToReplaceLogger, Level loggingLevel){

        Field loggerField = getModifiableLoggerField(classWhereToReplaceLogger);
        Logger originalLogger = getLoggerFromField(loggerField);
        try {

            ch.qos.logback.classic.Logger loggerToUseInFunction = createLogbackLogger(classWhereToReplaceLogger, loggingLevel);
            replaceLoggerField(loggerField, loggerToUseInFunction);

            processWithLoggerReplacementFunction.accept(loggerToUseInFunction);

            loggerToUseInFunction.detachAndStopAllAppenders();

        } finally {
            // revert back to original logger
            replaceLoggerField(loggerField, originalLogger);
        }
    }

    public static Field getModifiableLoggerField(Class<?> clazz){
        Field loggerField = Arrays.stream(clazz.getDeclaredFields()).filter(field -> org.slf4j.Logger.class.isAssignableFrom(field.getType()))
            .findFirst().orElseThrow(() -> new IllegalStateException("No logger found in class "+clazz.getName()));
        try {
            loggerField.setAccessible(true);
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(loggerField, loggerField.getModifiers() & ~Modifier.FINAL);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return loggerField;
    }

    public static Logger getLoggerFromField(Field loggerField){
        try {
            return (Logger) loggerField.get(null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void replaceLoggerField(Field loggerField, Logger logger){
        try {
            loggerField.set(null, logger);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static ListAppender<ILoggingEvent> registerListAppender(ch.qos.logback.classic.Logger logger){
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
        return listAppender;
    }
}
