package com.thinkaurelius.titan.net.msg;

/**
 *
 * @author dalaro
 */
public class Fault extends Message {

    private final Key seed;
    private final String message;

    public Fault(Key seed, String message) {
        this.seed = seed;
        this.message = message;
    }

    public Key getSeed() {
        return seed;
    }

    public String getMessage() {
        return message;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((message == null) ? 0 : message.hashCode());
		result = prime * result + ((seed == null) ? 0 : seed.hashCode());
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
		Fault other = (Fault) obj;
		if (message == null) {
			if (other.message != null)
				return false;
		} else if (!message.equals(other.message))
			return false;
		if (seed == null) {
			if (other.seed != null)
				return false;
		} else if (!seed.equals(other.seed))
			return false;
		return true;
	}    
}
