package com.thinkaurelius.titan.net.msg;

/**
 *
 * @author dalaro
 */
public class Ping extends Message {

    private final Key client;

    public Ping(Key client) {
        this.client = client;
    }

    public Key getClient() {
        return client;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((client == null) ? 0 : client.hashCode());
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
		Ping other = (Ping) obj;
		if (client == null) {
			if (other.client != null)
				return false;
		} else if (!client.equals(other.client))
			return false;
		return true;
	}
    
    
}
