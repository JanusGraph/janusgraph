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

import java.util.Map;

import static org.janusgraph.graphdb.configuration.JanusGraphConstants.UPGRADEABLE_FIXED;

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
        Preconditions.checkArgument(hasUpgradeableFixed(option.getName()) ||
                                    !option.isFixed() || !isFrozen(), "Cannot change configuration option: %s", option);
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

    private boolean hasUpgradeableFixed(String name) {
        return UPGRADEABLE_FIXED.contains(name);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return config;
    }
}
