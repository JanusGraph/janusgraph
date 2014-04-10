package com.thinkaurelius.titan.util.system;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

public class NetworkUtil {
    public static String getLoopbackAddress() {
        // InetAddress.getLoopbackAddress() is @since 1.7
        //
        // Aside from that, getLoopbackAddress() seems preferable to
        // InetAddress.getByName("localhost") since the former doesn't seem to
        // require the local resolver to be sane.
        //return InetAddress.getLoopbackAddress().getHostAddress();
        try {
            return InetAddress.getByName("localhost").getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
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
