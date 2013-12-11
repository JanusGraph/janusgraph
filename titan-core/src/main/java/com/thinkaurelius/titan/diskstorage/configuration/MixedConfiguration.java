package com.thinkaurelius.titan.diskstorage.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

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
    public<O> O get(ConfigOption<O> option, String... umbrellaElements) {
        String key = super.getPath(option,umbrellaElements);
        Object result = null;
        if (option.isLocal()) {
            result = local.get(key,option.getDatatype());
        }
        if (result==null && option.getType()!=ConfigOption.Type.LOCAL) {
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

    @Override
    public void close() {
        global.close();
        local.close();
    }
}
