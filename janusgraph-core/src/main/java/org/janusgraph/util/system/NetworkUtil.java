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

package org.janusgraph.util.system;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

public class NetworkUtil {
    public static String getLoopbackAddress() {
        return InetAddress.getLoopbackAddress().getHostAddress();
    }

    public static InetAddress getLocalHost() {
        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }

    public static String getLocalAddress() {
        return getLocalHost().getHostAddress();
    }

    public static String getLocalHostName() {
        return getLocalHost().getHostName();
    }

    public static boolean hasLocalAddress(Collection<String> endpoints) {
        return endpoints.contains(getLoopbackAddress()) || endpoints.contains(getLocalAddress()) || endpoints.contains(getLocalHostName());
    }

    public static boolean isLocalConnection(String hostname) {
        InetAddress localhost = NetworkUtil.getLocalHost();
        return hostname.equalsIgnoreCase(NetworkUtil.getLoopbackAddress())
                || hostname.equals(localhost.getHostAddress())
                || hostname.equals(localhost.getHostName())
                || hostname.equals(localhost.getCanonicalHostName());
    }
}
