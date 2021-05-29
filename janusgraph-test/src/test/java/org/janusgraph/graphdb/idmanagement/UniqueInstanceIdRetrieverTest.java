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

package org.janusgraph.graphdb.idmanagement;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.util.encoding.LongEncoding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UniqueInstanceIdRetrieverTest {

    private final UniqueInstanceIdRetriever uniqueInstanceIdRetriever = UniqueInstanceIdRetriever.getInstance();

    @Mock
    private Configuration config;

    @Test
    public void shouldGetUidAsUniqueInstanceId(){
        String uid = "uid";
        when(config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID)).thenReturn(true);
        when(config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID)).thenReturn(uid);
        String result = uniqueInstanceIdRetriever.getOrGenerateUniqueInstanceId(config);
        assertEquals(uid, result);
    }

    @Test
    public void shouldThrowExceptionOnIllegalUidCharacters(){
        String uid = "uid"+ConfigElement.ILLEGAL_CHARS[0];
        when(config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID)).thenReturn(true);
        when(config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID)).thenReturn(uid);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> uniqueInstanceIdRetriever.getOrGenerateUniqueInstanceId(config));
        assertEquals("Invalid unique identifier: "+uid, exception.getMessage());
    }

    @Test
    @MockitoSettings(strictness= Strictness.LENIENT)
    public void shouldComputeUidSuffixWithUniqueInstanceIdSuffix(){
        short uniqueInstanceIdSuffix = (short)123;
        when(config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_SUFFIX)).thenReturn(true);
        when(config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_SUFFIX)).thenReturn(uniqueInstanceIdSuffix);
        String result = uniqueInstanceIdRetriever.getOrGenerateUniqueInstanceId(config);
        assertTrue(result.endsWith(LongEncoding.encode(uniqueInstanceIdSuffix)));
    }

    @Test
    public void shouldComputeUidSuffixWithRuntimeMXBeanName(){
        String result = uniqueInstanceIdRetriever.getOrGenerateUniqueInstanceId(config);
        String runtimeMXBean = replaceIllegalChars(ManagementFactory.getRuntimeMXBean().getName());
        assertTrue(result.contains(runtimeMXBean));
    }

    @Test
    @MockitoSettings(strictness= Strictness.LENIENT)
    public void shouldComputeUidWithUniqueInstanceIdHostname() throws UnknownHostException {
        when(config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME)).thenReturn(true);
        when(config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME)).thenReturn(true);
        String result = uniqueInstanceIdRetriever.getOrGenerateUniqueInstanceId(config);
        String hostName = replaceIllegalChars(Inet4Address.getLocalHost().getHostName());
        assertEquals(result, hostName);
    }

    @Test
    @MockitoSettings(strictness= Strictness.LENIENT)
    public void shouldComputeUidWithUniqueInstanceIdAddress() throws UnknownHostException {
        when(config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME)).thenReturn(true);
        when(config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME)).thenReturn(false);
        String result = uniqueInstanceIdRetriever.getOrGenerateUniqueInstanceId(config);
        String hostName = new String(Hex.encodeHex(Inet4Address.getLocalHost().getAddress()));
        hostName = replaceIllegalChars(hostName);
        assertEquals(result, hostName);
    }

    private String replaceIllegalChars(String str){
        for (char c : ConfigElement.ILLEGAL_CHARS) {
            str = StringUtils.replaceChars(str,c,'-');
        }
        return str;
    }
}
