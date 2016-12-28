package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MixedConfiguration extends AbstractConfiguration {

    private final ReadConfiguration global;
    private final ReadConfiguration local;

    public MixedConfiguration(ConfigNamespace root, ReadConfiguration global, ReadConfiguration local) {
        super(root);
        Preconditions.checkNotNull(global);
        Preconditions.checkNotNull(local);
        this.global = global;
        this.local = local;
    }

    @Override
    public boolean has(ConfigOption option, String... umbrellaElements) {
        String key = super.getPath(option,umbrellaElements);
        if (option.isLocal() && local.get(key,option.getDatatype())!=null) return true;
        if (option.isGlobal() && global.get(key,option.getDatatype())!=null) return true;
        return false;
    }

    @Override
    public<O> O get(ConfigOption<O> option, String... umbrellaElements) {
        String key = super.getPath(option,umbrellaElements);
        Object result = null;
        if (option.isLocal()) {
            result = local.get(key,option.getDatatype());
        }
        if (result==null && option.isGlobal()) {
            result = global.get(key,option.getDatatype());
        }
        return option.get(result);
    }

    @Override
    public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements) {
        Set<String> result = Sets.newHashSet();
        for (ReadConfiguration config : new ReadConfiguration[]{global,local}) {
            result.addAll(super.getContainedNamespaces(config,umbrella,umbrellaElements));
        }
        return result;
    }

    public Map<String,Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements) {
        Map<String,Object> result = Maps.newHashMap();
        for (ReadConfiguration config : new ReadConfiguration[]{global,local}) {
            result.putAll(super.getSubset(config,umbrella,umbrellaElements));
        }
        return result;
    }

    @Override
    public Configuration restrictTo(String... umbrellaElements) {
        return restrictTo(this,umbrellaElements);
    }

    @Override
    public void close() {
        global.close();
        local.close();
    }
}
