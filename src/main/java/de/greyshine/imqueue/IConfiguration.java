package de.greyshine.imqueue;

import java.io.File;
import java.io.PrintStream;

public interface IConfiguration {
	
    int DEFAULT_MAX_RECEIVER_THREADS = 1;

    /**
     * 
     */
    long DEFAULT_MAX_RECEIVER_THREAD_TIMERTOLIVE = 3 * 10 * 1000;
    /**
     * 
     */
    long DEFAULT_MAX_RECEIVER_THREAD_EXECUTIONS = 100;

    /**
     * 10 seks
     */
    long DEFAULT_THREAD_WAIT_TIME = 10*1000;

    Class<? extends IReceiver> getReceiverClass();
    
    default int getMaxReceiverThreads() {
    	return DEFAULT_MAX_RECEIVER_THREADS;
    }

    default long getThreadWaitTimeout() {
    	return DEFAULT_THREAD_WAIT_TIME;
    }

    /**
     * Amount of how many times a receiver thread handles a message
     * @return
     */
    default long getMaxReceiverThreadExecutions() {
    	return DEFAULT_MAX_RECEIVER_THREAD_EXECUTIONS;
    }

    
    default long getMaxReceiverThreadTimeToLive() {
    	return DEFAULT_MAX_RECEIVER_THREAD_TIMERTOLIVE;
    }

    /**
     * when <code>false</code> the queue will be worked off by the handlers
     * @return
     */
    default boolean serializeMessagesOnClose() {
    	return false;
    }

    default PrintStream getLogStream() {
    	return null;
    }
    
    default <T extends IReceiver> IReceiverFactory<T> getReceiverFactory(Class<T> inReceiverClass) {
    	return new DefaultReceiverFactory<T>( inReceiverClass );
    }

    /**
     * Amount of exceptional receive tries are executed.
     * @return
     */
	default int getMaxReceiveAttempts() {
		return 0;
	};
	
	/**
	 * Time to wait between exceptional receive attempts
	 * @return
	 */
	default long getTimeBetweenReceiveAttempts() {
		return 10 * 1000;
	}
	
	default File getSerialisationDirectory() {
		return new File(".", InMemoryQueue.class.getSimpleName()+File.separator+getReceiverClass().getTypeName());
	}

}
