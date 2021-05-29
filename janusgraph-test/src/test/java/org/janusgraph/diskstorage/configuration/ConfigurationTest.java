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

import com.google.common.collect.ImmutableSet;
import org.janusgraph.core.util.ReflectiveConfigOptionLoader;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.Test;

import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConfigurationTest {


    @Test
    public void testConfigHierarchy() {
        ConfigNamespace root = new ConfigNamespace(null,"config","root");
        ConfigNamespace indexes = new ConfigNamespace(root,"indexes","Index definitions",true);
        ConfigNamespace storage = new ConfigNamespace(root,"storage","Storage definitions");
        ConfigNamespace special = new ConfigNamespace(storage,"special","Special storage definitions");
        ConfigOption<String[]> hostnames = new ConfigOption<>(storage, "hostname", "Storage backend hostname",
                ConfigOption.Type.LOCAL, String[].class);
        ConfigOption<Boolean> partition = new ConfigOption<>(storage, "partition", "whether to enable partition",
                ConfigOption.Type.MASKABLE, false);
        ConfigOption<Long> locktime = new ConfigOption<>(storage,"locktime","how long to lock",
                ConfigOption.Type.FIXED, 500L);
        ConfigOption<Byte> bits = new ConfigOption<>(storage,"bits","number of unique bits",
                ConfigOption.Type.GLOBAL_OFFLINE, (byte)8);
        ConfigOption<Short> retry = new ConfigOption<>(special,"retry","retry wait time",
                ConfigOption.Type.GLOBAL, (short)200);
        ConfigOption<Double> bar = new ConfigOption<>(special,"bar","bar",
                ConfigOption.Type.GLOBAL, 1.5d);
        ConfigOption<Integer> bim = new ConfigOption<>(special, "bim", "bim",
                ConfigOption.Type.MASKABLE, Integer.class);

        ConfigOption<String> indexback = new ConfigOption<>(indexes, "name", "index name",
                ConfigOption.Type.MASKABLE, String.class);
        ConfigOption<Integer> ping = new ConfigOption<>(indexes, "ping", "ping time",
                ConfigOption.Type.LOCAL, 100);
        ConfigOption<Boolean> presort = new ConfigOption<>(indexes, "presort", "presort result set",
                ConfigOption.Type.LOCAL, false);

        //Local configuration
        ModifiableConfiguration config = new ModifiableConfiguration(root,new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration()), BasicConfiguration.Restriction.LOCAL);
        UserModifiableConfiguration userconfig = new UserModifiableConfiguration(config);
        assertFalse(config.get(partition));
        assertEquals("false", userconfig.get("storage.partition"));
        userconfig.set("storage.partition", true);
        assertEquals("true", userconfig.get("storage.partition"));
        userconfig.set("storage.hostname", new String[]{"localhost", "some.where.org"});
        assertEquals("[localhost,some.where.org]", userconfig.get("storage.hostname"));
        userconfig.set("storage.hostname", "localhost");
        assertEquals("[localhost]", userconfig.get("storage.hostname"));
        assertEquals("null", userconfig.get("storage.special.bim"));
        assertEquals("", userconfig.get("indexes"));
        userconfig.set("indexes.search.name", "foo");
        assertEquals("+ search", userconfig.get("indexes").trim());
        assertEquals("foo", userconfig.get("indexes.search.name"));
        assertEquals("100", userconfig.get("indexes.search.ping"));
        userconfig.set("indexes.search.ping", 400L);
        assertEquals("400",userconfig.get("indexes.search.ping"));
        assertFalse(config.isFrozen());
        try {
            userconfig.set("storage.locktime",500);
            fail();
        } catch (IllegalArgumentException ignored) {}
        try {
            config.set(retry,(short)100);
            fail();
        } catch (IllegalArgumentException ignored) {}
//        System.out.println(userconfig.get("storage"));
        userconfig.close();
        ReadConfiguration localConfig = userconfig.getConfiguration();

        config = new ModifiableConfiguration(root,new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration()), BasicConfiguration.Restriction.GLOBAL);
        userconfig = new UserModifiableConfiguration(config);

        userconfig.set("storage.locktime",1111);
        userconfig.set("storage.bits",5);
        userconfig.set("storage.special.retry",222);
        assertEquals("5", userconfig.get("storage.bits"));
        assertEquals("222", userconfig.get("storage.special.retry"));

        config.freezeConfiguration();
        userconfig.set("storage.special.retry", 333);
        assertEquals("333", userconfig.get("storage.special.retry"));
        try {
            userconfig.set("storage.bits",6);
        } catch (IllegalArgumentException ignored) {}
        userconfig.set("storage.bits",6);
        try {
            userconfig.set("storage.locktime",1221);
        } catch (IllegalArgumentException ignored) {}
        try {
            userconfig.set("storage.locktime",1221);
        } catch (IllegalArgumentException ignored) {}
        userconfig.set("indexes.find.name","lulu");

        userconfig.close();
        ReadConfiguration globalConfig = userconfig.getConfiguration();

        MixedConfiguration mixed = new MixedConfiguration(root,globalConfig,localConfig);
        assertEquals(ImmutableSet.of("search","find"),mixed.getContainedNamespaces(indexes));
        Configuration search = mixed.restrictTo("search");
        assertEquals("foo",search.get(indexback));
        assertEquals(400,search.get(ping).intValue());
        assertEquals(100,mixed.get(ping,"find").intValue());
        assertEquals(false, mixed.get(presort, "find"));
        assertEquals(400,mixed.get(ping,"search").intValue());
        assertEquals(false, mixed.get(presort, "search"));
        assertFalse(mixed.has(bim));
        assertTrue(mixed.has(bits));
        assertEquals(5,mixed.getSubset(storage).size());

        assertEquals(1.5d, (double)mixed.get(bar));
        assertEquals("localhost",mixed.get(hostnames)[0]);
        assertEquals(1111,mixed.get(locktime).longValue());

        mixed.close();

        //System.out.println(ConfigElement.toString(root));

    }

    @Test
    public void testDisableConfOptReflection() {
        ReflectiveConfigOptionLoader.INSTANCE.setEnabled(false);
        ReflectiveConfigOptionLoader.INSTANCE.loadStandard(this.getClass());

        assertFalse(StreamSupport.stream(GraphDatabaseConfiguration.LOG_NS.getChildren().spliterator(), false)
            .anyMatch(elem -> elem instanceof ConfigOption<?> && elem.getName().equals("max-write-time")));

        ReflectiveConfigOptionLoader.INSTANCE.setEnabled(true);
        ReflectiveConfigOptionLoader.INSTANCE.loadStandard(this.getClass());

        assertTrue(StreamSupport.stream(GraphDatabaseConfiguration.LOG_NS.getChildren().spliterator(), false)
            .anyMatch(elem -> elem instanceof ConfigOption<?> && elem.getName().equals("max-write-time")));
    }
}
