package com.thinkaurelius.titan.net.msg;

import java.util.Collection;

/**
 *
 * @author dalaro
 */
public class Pong extends Message {

    private final Key key;
    private Collection<Long> querySeedIds;

    public Pong(Key key, Collection<Long> queryKeyIds) {
        this.key = key;
        this.querySeedIds = queryKeyIds;
    }

    public Key getKey() {
        return key;
    }

    public Collection<Long> getQueryKeyIds() {
        return querySeedIds;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result
				+ ((querySeedIds == null) ? 0 : querySeedIds.hashCode());
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
		Pong other = (Pong) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (querySeedIds == null) {
			if (other.querySeedIds != null)
				return false;
		} else if (!querySeedIds.equals(other.querySeedIds))
			return false;
		return true;
	}
    
    
}
