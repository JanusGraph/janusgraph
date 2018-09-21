// Copyright 2018 JanusGraph Authors
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

import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.ArrayUtils.toArray;

abstract class AbstractJanusGraphTestSuite extends Suite {

    AbstractJanusGraphTestSuite(Class<?> klass, RunnerBuilder builder, Class<?>[] allTests) throws InitializationError {
        super(builder, klass, extend(klass, allTests));

        final Class<? extends JanusGraphDatabaseProvider> janusGraphDatabaseProvider = getJanusGraphDatabaseProvider(klass);
        registerOptOuts(klass);

        try {
            JanusGraphDatabaseProvider provider = janusGraphDatabaseProvider.newInstance();
            JanusGraphDatabaseManager.setGraphDatabaseProvider(provider);
        } catch (Exception ex) {
            throw new InitializationError(ex);
        }

    }

    private static Class<?>[] extend(Class<?> klass, Class<?>[] allTests) throws InitializationError {
        final JanusGraphSpecificTestClass[] extraTests = klass.getAnnotationsByType(JanusGraphSpecificTestClass.class);
        List<Class<?>> collect = Arrays.stream(extraTests).map(JanusGraphSpecificTestClass::testClass).collect(Collectors.toList());
        Collections.addAll(collect, allTests);
        return collect.toArray(new Class<?>[collect.size()]);
    }

    private void registerOptOuts(Class<?> klass) throws InitializationError {
        final JanusGraphIgnoreTest[] ignoreTests = klass.getAnnotationsByType(JanusGraphIgnoreTest.class);
        if (ignoreTests != null && ignoreTests.length > 0) {
            // validate annotation - test class and reason must be set
            if (!Arrays.stream(ignoreTests).allMatch(ignore -> ignore.test() != null && ignore.reason() != null && !ignore.reason().isEmpty()))
                throw new InitializationError("Check @JanusGraphIgnoreTest annotations - all must have a 'test' and 'reason' set");

            try {
                filter(new IngoreTestFilter(ignoreTests, klass.getAnnotationsByType(JanusGraphSpecificTestClass.class)));
            } catch (NoTestsRemainException ex) {
                throw new InitializationError(ex);
            }
        }
    }

    private static Class<? extends JanusGraphDatabaseProvider> getJanusGraphDatabaseProvider(final Class<?> klass) throws InitializationError {
        final JanusGraphProviderClass annotation = klass.getAnnotation(JanusGraphProviderClass.class);
        if (null == annotation)
            throw new InitializationError(String.format("class '%s' must have a JanusGraphProviderClass annotation", klass.getName()));
        return annotation.provider();
    }

    public static class IngoreTestFilter extends Filter {


        /**
         * Ignores a specific test in a specific test case.
         */
        private final List<Description> individualSpecificTestsToIgnore;

        /**
         * Ignores an entire specific test case.
         */
        private final List<JanusGraphIgnoreTest> entireTestCaseToIgnore;

        /**
         *
         */
        private final List<Class<?>> specificTestClasses;

        private IngoreTestFilter(JanusGraphIgnoreTest[] ignoreTests, JanusGraphSpecificTestClass[] testClasses) {
            final Map<Boolean, List<JanusGraphIgnoreTest>> split = Arrays.stream(ignoreTests)
                .collect(Collectors.groupingBy(optOut -> optOut.method().equals("*")));

            final List<JanusGraphIgnoreTest> optOutsOfIndividualTests = split.getOrDefault(Boolean.FALSE, Collections.emptyList());

            individualSpecificTestsToIgnore = optOutsOfIndividualTests.stream()
                .filter(ignoreTest -> !ignoreTest.method().equals("*"))
                .<org.javatuples.Pair>map(ignoreTest -> org.javatuples.Pair.with(ignoreTest.test(), ignoreTest.specific().isEmpty() ?
                    ignoreTest.method() :
                    String.format("%s[%s]", ignoreTest.method(), ignoreTest.specific())))
                .map(p -> Description.createTestDescription(p.getValue0().toString(), p.getValue1().toString()))
                .collect(Collectors.toList());

            entireTestCaseToIgnore = split.getOrDefault(Boolean.TRUE, Collections.emptyList());

            specificTestClasses = Arrays.stream(testClasses).map(JanusGraphSpecificTestClass::testClass).collect(Collectors.toList());
        }

        @Override
        public boolean shouldRun(Description description) {
            if (description.getTestClass() != null) {
                final boolean ignoreWholeTestCase = entireTestCaseToIgnore.stream().map(this::transformToClass)
                    .anyMatch(claxx -> claxx.isAssignableFrom(description.getTestClass()));

                if (ignoreWholeTestCase) {
                    boolean shouldRunTestClass = specificTestClasses.stream().anyMatch(claxx -> claxx.isAssignableFrom(description.getTestClass()));
                    if (!shouldRunTestClass) {
                        return false;
                    }
                }
            }
            if (description.isTest()) {
                // next check if there is a test group to consider. if not then check for a  specific test to ignore
                return !individualSpecificTestsToIgnore.contains(description);
            }
            return true;
        }

        @Override
        public String describe() {
            return String.format("Method %s",
                String.join(",", individualSpecificTestsToIgnore.stream().map(Description::getDisplayName).collect(Collectors.toList())));
        }

        private Class<?> transformToClass(final JanusGraphIgnoreTest optOut) {
            try {
                return Class.forName(optOut.test());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
