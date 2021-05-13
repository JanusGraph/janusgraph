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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BasicConfiguration extends AbstractConfiguration {

    private static final Logger log =
        LoggerFactory.getLogger(BasicConfiguration.class);

    public enum Restriction { LOCAL, GLOBAL, NONE }

    protected static final String FROZEN_KEY = "hidden.frozen";

    private final ReadConfiguration config;
    private final Restriction restriction;
    private Boolean isFrozen;

    public BasicConfiguration(ConfigNamespace root, ReadConfiguration config, Restriction restriction) {
        super(root);
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(restriction);
        this.config = config;
        this.restriction = restriction;
    }

    protected void verifyOption(ConfigOption option) {
        Preconditions.checkNotNull(option);
        super.verifyElement(option);
        if (restriction==Restriction.GLOBAL) Preconditions.checkArgument(option.isGlobal(),"Can only accept global options: %s",option);
        else if (restriction==Restriction.LOCAL) Preconditions.checkArgument(option.isLocal(),"Can only accept local options: %s",option);
    }


    @Override
    public boolean has(ConfigOption option, boolean includeRoot, String... umbrellaElements) {
        verifyOption(option);
        return config.get(super.getPath(option, includeRoot, umbrellaElements),option.getDatatype())!=null;
    }

    @Override
    public<O> O get(ConfigOption<O> option, boolean includeRoot, String... umbrellaElements) {
        verifyOption(option);
        O result = config.get(super.getPath(option, includeRoot, umbrellaElements),option.getDatatype());
        return option.get(result);
    }

    @Override
    public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements) {
        return super.getContainedNamespaces(config,umbrella,umbrellaElements);
    }

    @Override
    public Map<String, Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements) {
        return super.getSubset(config,umbrella,umbrellaElements);
    }

    @Override
    public Configuration restrictTo(String... umbrellaElements) {
        return restrictTo(this,umbrellaElements);
    }

    public Map<ConfigElement.PathIdentifier,Object> getAll() {
        Map<ConfigElement.PathIdentifier,Object> result = new HashMap<>();

        for (String key : config.getKeys("")) {
            Preconditions.checkArgument(StringUtils.isNotBlank(key));
            try {
                final ConfigElement.PathIdentifier pid = ConfigElement.parse(getRootNamespace(),key);
                Preconditions.checkArgument(pid.element.isOption() && !pid.lastIsUmbrella);
                result.put(pid, get((ConfigOption) pid.element, pid.umbrellaElements));
            } catch (IllegalArgumentException e) {
                log.debug("Ignored configuration entry for {} since it does not map to an option",key,e);
            }
        }
        return result;
    }

    public boolean isFrozen() {
        if (null == isFrozen) {
            Boolean frozen = config.get(FROZEN_KEY, Boolean.class);
            isFrozen = null == frozen ? false : frozen;
        }
        return isFrozen;
    }

    protected void setFrozen() {
        isFrozen = true;
    }

    public ReadConfiguration getConfiguration() {
        return config;
    }

    @Override
    public void close() {
        config.close();
    }
}
