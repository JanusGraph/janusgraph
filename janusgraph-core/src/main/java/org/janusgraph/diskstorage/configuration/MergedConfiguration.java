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

package org.janusgraph.diskstorage.configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MergedConfiguration implements Configuration {

    private final Configuration first;
    private final Configuration second;

    public MergedConfiguration(Configuration first, Configuration second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean has(ConfigOption option, boolean includeRoot, String... umbrellaElements) {
        return first.has(option, includeRoot, umbrellaElements) || second.has(option, includeRoot, umbrellaElements);
    }

    @Override
    public <O> O get(ConfigOption<O> option, boolean includeRoot, String... umbrellaElements) {
        if (first.has(option, includeRoot, umbrellaElements))
            return first.get(option, includeRoot, umbrellaElements);

        if (second.has(option, includeRoot, umbrellaElements))
            return second.get(option, includeRoot, umbrellaElements);

        return option.getDefaultValue();
    }
    @Override
    public Set<String> getContainedNamespaces(ConfigNamespace umbrella,
            String... umbrellaElements) {
        Set<String> firstContainedNamespaces = first.getContainedNamespaces(umbrella, umbrellaElements);
        Set<String> secondContainedNamespaces = second.getContainedNamespaces(umbrella, umbrellaElements);
        Set<String> b = new HashSet<>(firstContainedNamespaces.size()+secondContainedNamespaces.size());
        b.addAll(firstContainedNamespaces);
        b.addAll(secondContainedNamespaces);
        return Collections.unmodifiableSet(b);
    }
    @Override
    public Map<String, Object> getSubset(ConfigNamespace umbrella,
            String... umbrellaElements) {
        Map<String, Object> fm = first.getSubset(umbrella, umbrellaElements);
        Map<String, Object> sm = second.getSubset(umbrella, umbrellaElements);
        Map<String, Object> b = new HashMap<>(fm.size()+sm.size());
        // Order of insertions matter. We should have all elements from first subset and all elements
        // from second subset which don't have same key in first subset. Thus we are adding all elements from  the
        // second subset and then adding elements from first subset (which replaces elements with the same key from second subset)
        b.putAll(sm);
        b.putAll(fm);
        return Collections.unmodifiableMap(b);
    }
    @Override
    public Configuration restrictTo(String... umbrellaElements) {
        return new MergedConfiguration(first.restrictTo(umbrellaElements), second.restrictTo(umbrellaElements));
    }
}
