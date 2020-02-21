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

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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
        ImmutableSet.Builder<String> b = ImmutableSet.builder();
        b.addAll(first.getContainedNamespaces(umbrella, umbrellaElements));
        b.addAll(second.getContainedNamespaces(umbrella, umbrellaElements));
        return b.build();
    }
    @Override
    public Map<String, Object> getSubset(ConfigNamespace umbrella,
            String... umbrellaElements) {
        ImmutableMap.Builder<String, Object> b = ImmutableMap.builder();
        Map<String, Object> fm = first.getSubset(umbrella, umbrellaElements);
        Map<String, Object> sm = second.getSubset(umbrella, umbrellaElements);

        b.putAll(first.getSubset(umbrella, umbrellaElements));

        for (Map.Entry<String, Object> secondEntry : sm.entrySet()) {
            if (!fm.containsKey(secondEntry.getKey())) {
                b.put(secondEntry);
            }
        }

        return b.build();
    }
    @Override
    public Configuration restrictTo(String... umbrellaElements) {
        return new MergedConfiguration(first.restrictTo(umbrellaElements), second.restrictTo(umbrellaElements));
    }
}
