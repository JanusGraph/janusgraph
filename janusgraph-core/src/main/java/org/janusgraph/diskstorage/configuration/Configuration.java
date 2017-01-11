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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Configuration {

    public boolean has(ConfigOption option, String... umbrellaElements);

    public<O> O get(ConfigOption<O> option, String... umbrellaElements);

    public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements);

    public Map<String,Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements);

    public Configuration restrictTo(final String... umbrellaElements);


    //--------------------

    public static final Configuration EMPTY = new Configuration() {
        @Override
        public boolean has(ConfigOption option, String... umbrellaElements) {
            return false;
        }

        @Override
        public <O> O get(ConfigOption<O> option, String... umbrellaElements) {
            return option.getDefaultValue();
        }

        @Override
        public Set<String> getContainedNamespaces(ConfigNamespace umbrella, String... umbrellaElements) {
            return Sets.newHashSet();
        }

        @Override
        public Map<String, Object> getSubset(ConfigNamespace umbrella, String... umbrellaElements) {
            return Maps.newHashMap();
        }

        @Override
        public Configuration restrictTo(String... umbrellaElements) {
            return EMPTY;
        }
    };


}
