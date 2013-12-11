package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.base.Preconditions;

import java.util.Set;

/**
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ModifiableConfiguration extends AbstractConfiguration {

    private static final String FROZEN_KEY = "hidden.frozen";

    private final WriteConfiguration config;
    private final boolean isGlobal;
    private boolean isFrozen;

    public ModifiableConfiguration(ConfigNamespace root, WriteConfiguration config, boolean global) {
        super(root);
        Preconditions.checkNotNull(config);
        this.config = config;
        isGlobal = global;
        isFrozen = isFrozenConfiguration();
    }

    private void verifyOption(ConfigOption option) {
        Preconditions.checkNotNull(option);
        super.verifyElement(option);
        if (isGlobal) Preconditions.checkArgument(option.isGlobal(),"Can only accept global options: %s",option);
        else Preconditions.checkArgument(option.isLocal(),"Can only accept local options: %s",option);
    }

    public<O> void set(ConfigOption<O> option, O value, String... umbrellaElements) {
        verifyOption(option);
        Preconditions.checkArgument(!option.isFixed() || !isFrozen, "Cannot change configuration option: %s", option);
        String key = super.getPath(option,umbrellaElements);
        value = option.verify(value);
        config.set(key,value);
    }

    @Override
    public<O> O get(ConfigOption<O> option, String... umbrellaElements) {
        verifyOption(option);
        O result = config.get(super.getPath(option,umbrellaElements),option.getDatatype());
        return option.get(result);
    }

    @Override
    public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements) {
        return super.getContainedNamespaces(config,umbrella,umbrellaElements);
    }

    public void freezeConfiguration() {
        if (!isFrozen) config.set(FROZEN_KEY,Boolean.TRUE);
        isFrozen = true;
    }

    public boolean isFrozenConfiguration() {
        Boolean frozen = config.get(FROZEN_KEY,Boolean.class);
        if (frozen==null) return false;
        else return frozen;
    }

    public WriteConfiguration getBackingConfiguration() {
        return config;
    }


    @Override
    public void close() {
        config.close();
    }
}
