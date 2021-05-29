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

package org.janusgraph.graphdb.internal;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Token {

    public static final char SEPARATOR_CHAR = 0x1e;

    public static final String systemETprefix = Graph.Hidden.hide("T$");
    public static final String NONEXISTENT_TYPE = systemETprefix+"doesNotExist";

    public static String getSeparatedName(String... components) {
        for (String component : components) verifyName(component);
        return StringUtils.join(components,SEPARATOR_CHAR);
    }

    public static void verifyName(String name) {
        Preconditions.checkArgument(name.indexOf(Token.SEPARATOR_CHAR) < 0,
                "Name can not contains reserved character %s: %s", Token.SEPARATOR_CHAR, name);
    }

    public static String[] splitSeparatedName(String name) {
        return name.split(SEPARATOR_CHAR+"");
    }

    public static final String INTERNAL_INDEX_NAME = "internalindex";

    public static boolean isSystemName(String name) {
        return Graph.Hidden.isHidden(name);
    }

    public static String makeSystemName(String name) {
        return Graph.Hidden.hide(name);
    }



}
