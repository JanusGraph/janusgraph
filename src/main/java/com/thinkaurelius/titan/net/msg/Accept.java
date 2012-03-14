package com.thinkaurelius.titan.net.msg;

/**
 *
 * @author dalaro
 */
public class Accept extends Message {

    private final Key instance;

    public Accept(Key instance) {
        this.instance = instance;
    }

    public Key getInstance() {
        return instance;
    }
    
	@Override
	public String toString() {
		return "Accept[instance=" + instance + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((instance == null) ? 0 : instance.hashCode());
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
		Accept other = (Accept) obj;
		if (instance == null) {
			if (other.instance != null)
				return false;
		} else if (!instance.equals(other.instance))
			return false;
		return true;
	}
}
