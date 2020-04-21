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

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    public boolean has(ConfigOption option, boolean includeRoot, String... umbrellaElements) {
        final String key = super.getPath(option, includeRoot, umbrellaElements);
        return option.isLocal() && local.get(key, option.getDatatype()) != null
              || option.isGlobal() && global.get(key, option.getDatatype()) != null;
    }

    @Override
    public<O> O get(ConfigOption<O> option, boolean includeRoot, String... umbrellaElements) {
        final String key = super.getPath(option, includeRoot, umbrellaElements);
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
        return Arrays.stream(new ReadConfiguration[]{global,local})
            .map(config -> super.getContainedNamespaces(config, umbrella, umbrellaElements))
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
    }

    public Map<String,Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements) {
        Map<String,Object> result = new HashMap<>();
        result.putAll(super.getSubset(global,umbrella,umbrellaElements));
        result.putAll(super.getSubset(local,umbrella,umbrellaElements));
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
