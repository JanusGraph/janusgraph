package com.thinkaurelius.titan.net.msg;

import java.net.InetSocketAddress;

/**
 *
 * @author dalaro
 */
public class Key implements Comparable<Key> {

    private final long id;
    private final InetSocketAddress host;
    private final long hostBootTime;

    public Key(long id, InetSocketAddress host, long hostBootTime) {
        this.id = id;
        this.host = host;
        this.hostBootTime = hostBootTime;
    }
    
    public long getId() {
        return id;
    }

    public InetSocketAddress getHost() {
        return host;
    }
    
    public long getHostBootTime() {
    	return hostBootTime;
    }

    @Override
    public String toString() {
        return "Key{" + id + "," + host + ',' + hostBootTime + '}';
    }
    
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + (int) (hostBootTime ^ (hostBootTime >>> 32));
		result = prime * result + (int) (id ^ (id >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Key other = (Key) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (hostBootTime != other.hostBootTime)
			return false;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
    public int compareTo(Key other) {
        byte[] a = getHost().getAddress().getAddress();
        byte[] o = other.getHost().getAddress().getAddress();

        assert 4 == a.length;
        assert 4 == o.length;

        for (int i = 0; i < 4; i++) {
            if (a[i] < o[i]) {
                return -1;
            } else if (a[i] > o[i]) {
                return 1;
            }
        }
        
        long abt = getHostBootTime();
        long obt = other.getHostBootTime();
        
        if (abt < obt)
        	return -1;
        if (abt > obt)
        	return 1;
        
        long ats = getId();
        long ots = other.getId();

        if (ats < ots) {
            return -1;
        }
        if (ats > ots) {
            return 1;
        }

        return 0;
    }
}
