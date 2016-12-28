package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ModifiableConfiguration extends BasicConfiguration {

    private final WriteConfiguration config;

    public ModifiableConfiguration(ConfigNamespace root, WriteConfiguration config, Restriction restriction) {
        super(root, config, restriction);
        Preconditions.checkNotNull(config);
        this.config = config;
    }

    public<O> ModifiableConfiguration set(ConfigOption<O> option, O value, String... umbrellaElements) {
        verifyOption(option);
        Preconditions.checkArgument(!option.isFixed() || !isFrozen(), "Cannot change configuration option: %s", option);
        String key = super.getPath(option,umbrellaElements);
        value = option.verify(value);
        config.set(key,value);
        return this;
    }

    public void setAll(Map<ConfigElement.PathIdentifier,Object> options) {
        for (Map.Entry<ConfigElement.PathIdentifier,Object> entry : options.entrySet()) {
            Preconditions.checkArgument(entry.getKey().element.isOption());
            set((ConfigOption) entry.getKey().element, entry.getValue(), entry.getKey().umbrellaElements);
        }
    }

    public<O> void remove(ConfigOption<O> option, String... umbrellaElements) {
        verifyOption(option);
        Preconditions.checkArgument(!option.isFixed() || !isFrozen(), "Cannot change configuration option: %s", option);
        String key = super.getPath(option,umbrellaElements);
        config.remove(key);
    }

    public void freezeConfiguration() {
        config.set(FROZEN_KEY, Boolean.TRUE);
        if (!isFrozen()) setFrozen();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return config;
    }
}
