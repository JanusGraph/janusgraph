// Copyright 2018 JanusGraph Authors
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

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.JanusGraphConfigurationException;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation for unique instance identifier
 */
public class UniqueInstanceIdRetriever {

    private static final Logger log = LoggerFactory.getLogger(UniqueInstanceIdRetriever.class);

    private static UniqueInstanceIdRetriever uniqueInstanceIdGenerator;

    private final AtomicLong instanceCounter = new AtomicLong(0);

    private UniqueInstanceIdRetriever(){}

    public static synchronized UniqueInstanceIdRetriever getInstance(){
        if(uniqueInstanceIdGenerator == null){
            uniqueInstanceIdGenerator = new UniqueInstanceIdRetriever();
        }
        return uniqueInstanceIdGenerator;
    }

    public String getOrGenerateUniqueInstanceId(Configuration config) {
        String uid;
        if (!config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID)) {
            uid = computeUniqueInstanceId(config);
            log.info("Generated {}={}", GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID.getName(), uid);
        } else {
            uid = config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID);
        }
        Preconditions.checkArgument(!StringUtils.containsAny(uid, ConfigElement.ILLEGAL_CHARS),"Invalid unique identifier: %s",uid);
        return uid;
    }

    private String computeUniqueInstanceId(Configuration config) {
        final String suffix = getSuffix(config);
        final String uid = getUid(config);
        return ConfigElement.replaceIllegalChars(uid + suffix);
    }

    private String getSuffix(Configuration config) {
        final String suffix;
        if (config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_SUFFIX)) {
            suffix = LongEncoding.encode(config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_SUFFIX));
        } else if (!config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME)) {
            suffix = ManagementFactory.getRuntimeMXBean().getName() + LongEncoding.encode(instanceCounter.incrementAndGet());
        } else {
            suffix = "";
        }
        return suffix;
    }

    private String getUid(Configuration config) {
        final InetAddress localHost;
        try {
            localHost = Inet4Address.getLocalHost();
        } catch (UnknownHostException e) {
            throw new JanusGraphConfigurationException("Cannot determine local host", e);
        }
        final String uid;
        if (config.has(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME)
            && config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID_HOSTNAME)) {
            uid = localHost.getHostName();
        } else {
            final byte[] addrBytes = localHost.getAddress();
            uid = new String(Hex.encodeHex(addrBytes));
        }
        return uid;
    }

}
