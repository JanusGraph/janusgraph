package com.thinkaurelius.titan.util.system;

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
}
