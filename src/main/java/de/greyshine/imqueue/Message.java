package de.greyshine.imqueue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class Message implements Serializable {
	
	private static final long serialVersionUID = -8642390244456648744L;

	final long creationTime = System.currentTimeMillis();
	Long minSendTime = creationTime-1;
	
	long start;
	Long end;
	
	public final Serializable data;

	public final List<Exception> exceptions = new ArrayList<>(0);

	Message(Serializable data) {
		
		this.data = data;
	}
	
	@Override
    public String toString() {
        return getClass().getSimpleName() +" [creationTime=" + creationTime + ", start=" + start + ", end=" + end + ", exception="
                + ( exceptions.isEmpty()?"":exceptions.get( exceptions.size()-1 ) ) + ", data=" + data + "]";
    }
}