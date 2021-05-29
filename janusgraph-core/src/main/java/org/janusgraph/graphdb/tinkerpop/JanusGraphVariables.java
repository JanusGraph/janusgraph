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

package org.janusgraph.graphdb.tinkerpop;

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphVariables implements Graph.Variables {

    private final WriteConfiguration config;

    public JanusGraphVariables(WriteConfiguration config) {
        this.config = config;
    }

    @Override
    public Set<String> keys() {
        Set<String> set = new HashSet<>();
        config.getKeys("").forEach(set::add);
        return set;
    }

    @Override
    public <R> Optional<R> get(String s) {
        if (s==null) throw Graph.Variables.Exceptions.variableKeyCanNotBeNull();
        if (StringUtils.isEmpty(s)) throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
        Object value = config.get(s,Object.class);
        if (value==null) return Optional.empty();
        else return Optional.of((R)value);
    }

    @Override
    public void set(String s, Object o) {
        if (s==null) throw Graph.Variables.Exceptions.variableKeyCanNotBeNull();
        if (StringUtils.isEmpty(s)) throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
        if (o==null) throw Graph.Variables.Exceptions.variableValueCanNotBeNull();
        config.set(s,o);
    }

    @Override
    public void remove(String s) {
        if (s==null) throw Graph.Variables.Exceptions.variableKeyCanNotBeNull();
        if (StringUtils.isEmpty(s)) throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
        config.remove(s);
    }

    @Override
    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
