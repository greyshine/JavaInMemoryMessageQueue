package de.greyshine.imqueue;

import java.io.Serializable;
import java.util.List;

/**
 * Instances of receivers handle messages. 
 *
 */
public interface IReceiver {

	void handle(Serializable inMsg);
	
	default void handleException(List<Exception> inExceptions, Serializable inMsg) {}
	
	/**
	 * Callback method before the receiver instance is removed from the queue.
	 */
	default void terminate() {}
}
