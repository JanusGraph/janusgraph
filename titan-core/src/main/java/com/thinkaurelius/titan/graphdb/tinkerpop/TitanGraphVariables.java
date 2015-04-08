package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.google.common.collect.Sets;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanGraphVariables implements Graph.Variables {

    private final WriteConfiguration config;

    public TitanGraphVariables(WriteConfiguration config) {
        this.config = config;
    }

    @Override
    public Set<String> keys() {
        return Sets.newHashSet(config.getKeys(""));
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
