// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.diskstorage.configuration.converter;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration2.BaseConfiguration;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ReadConfigurationConverterTest {

    private final ReadConfigurationConverter readConfigurationConverter = ReadConfigurationConverter.getInstance();

    @Mock
    private ReadConfiguration readConfiguration;

    @Test
    public void shouldConvert(){

        Map<String, Object> configurationProperties = new HashMap<>();
        configurationProperties.put("key1", 123L);
        configurationProperties.put("key2", "value2");
        configurationProperties.put("key3", ImmutableSet.of("value3_1", "value3_2"));

        when(readConfiguration.getKeys("")).thenReturn(configurationProperties.keySet());

        when(readConfiguration.get(any(), eq(Object.class))).thenAnswer(invocation -> {
            Object key = invocation.getArguments()[0];
            return configurationProperties.get(key);
        });

        BaseConfiguration baseConfiguration = readConfigurationConverter.convert(readConfiguration);

        Iterator<String> keyIterator = baseConfiguration.getKeys();
        int propertiesAmount = 0;

        while (keyIterator.hasNext()){
            String key = keyIterator.next();
            assertTrue(configurationProperties.containsKey(key));

            Object expectedProperty = configurationProperties.get(key);
            Object actualProperty = baseConfiguration.getProperty(key);

            if(Collection.class.isAssignableFrom(expectedProperty.getClass())){
                assertTrue(CollectionUtils.isEqualCollection((Collection) expectedProperty, (Collection) actualProperty));
            } else {
                assertEquals(expectedProperty, actualProperty);
            }

            ++propertiesAmount;
        }

        assertEquals(configurationProperties.size(), propertiesAmount);
    }

}
