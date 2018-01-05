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

package org.janusgraph.util.datastructures;

/**
 * Utility class for analyzing exceptions
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExceptionUtil {

    public static boolean isCausedBy(Throwable exception, Class<?> exType) {
        Throwable ex2 = exception.getCause();
        return ex2 != null && ex2 != exception && (exType.isInstance(ex2) || isCausedBy(ex2, exType));
    }

}
