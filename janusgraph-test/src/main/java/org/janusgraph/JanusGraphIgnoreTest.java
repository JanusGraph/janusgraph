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

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(JanusGraphIgnoreTests.class)
@Inherited
public @interface JanusGraphIgnoreTest {
    /**
     * The test class to opt out of. This may be set to a base class of a test as in the case of the Gremlin
     * process class of tests from which Gremlin flavors extend.  If the actual test class is an inner class
     * of then use a "$" as a separator between the outer class and inner class.
     */
    public String test();

    /**
     * The specific name of the test method to opt out of or asterisk to opt out of all methods in a
     * {@link #test}.
     */
    public String method();

    /**
     * The reason the implementation is opting out of this test.
     */
    public String reason();

    /**
     * For parameterized tests specify the name of the test itself without its "square brackets".
     */
    public String specific() default "";
}

