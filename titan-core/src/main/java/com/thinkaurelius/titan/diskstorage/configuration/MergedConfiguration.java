package com.thinkaurelius.titan.diskstorage.configuration;

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
    public boolean has(ConfigOption option, String... umbrellaElements) {
        return first.has(option, umbrellaElements) || second.has(option, umbrellaElements);
    }

    @Override
    public <O> O get(ConfigOption<O> option, String... umbrellaElements) {
        if (first.has(option, umbrellaElements))
            return first.get(option, umbrellaElements);

        if (second.has(option, umbrellaElements))
            return second.get(option, umbrellaElements);

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
