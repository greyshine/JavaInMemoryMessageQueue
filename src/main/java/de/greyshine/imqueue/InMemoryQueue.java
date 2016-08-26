package de.greyshine.imqueue;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InMemoryQueue {

    private IConfiguration configuration;
    private List<Item> messages = new ArrayList<>();

    private boolean isCloseSignal = false;
    private boolean isClosed = false;
    private final Class<? extends IReceiver> receiverClass;

    private volatile long countThreadsCreated = 0;
    final Set<ReceiverThread> receiverThreadPool = new HashSet<>();
    
    private volatile long handlerThreadCount = 0;
    
    private final IStatus status = new IStatus() {

		@Override
		public int getCountMessagesOnQueue() {
			synchronized ( messages ) {
				return messages.size();
			}
		}

		@Override
		public long getCountReceiverThreadsCreated() {
			return countThreadsCreated;
		}
    	
    };

    public InMemoryQueue(Class<? extends IReceiver> inReceiverClass) {
        this( null, inReceiverClass );
    }
    
    public InMemoryQueue(IConfiguration inConfiguration, Class<? extends IReceiver> inReceiverClass) {

        configuration = inConfiguration;

        receiverClass = inReceiverClass;

        try {

            instantiateReceiver();

        } catch (Exception e) {

        	close();
            throw new IllegalArgumentException("Cannot instantiate Receiver: " + inReceiverClass, e);
        }
    }
    
    public IStatus getStatus() {
    	
    	return status;
    }

    public void setConfiguration(final IConfiguration inConfiguration) {

        // create a failsafe configuration
        configuration = new IConfiguration() {

            @Override
            public PrintStream getLogStream() {

                return inConfiguration == null ? null : inConfiguration.getLogStream();
            }

            @Override
            public boolean serializeMessagesOnClose() {

                if ( inConfiguration != null && inConfiguration.serializeMessagesOnClose() == true ) {
                    throw new IllegalArgumentException("serializing messages is not supported");
                }

                return false;
            }

            @Override
            public long getThreadWaitTimeout() {

                try {

                    final long v = inConfiguration.getThreadWaitTimeout();

                    return v < 1 ? DEFAULT_THREAD_WAIT_TIME : v;

                } catch(Exception e) {

                    return DEFAULT_THREAD_WAIT_TIME;
                }

            }

            @Override
            public int getMaxReceiverThreads() {

                try {
                    final int v = inConfiguration.getMaxReceiverThreads();

                    return v < 1 ? DEFAULT_MAX_RECEIVER_THREADS : v;

                } catch(Exception e) {

                    return DEFAULT_MAX_RECEIVER_THREADS;
                }
            }

            @Override
            public long getMaxReceiverThreadTimeToLive() {

                try {

                    final long v = inConfiguration.getMaxReceiverThreadTimeToLive();

                    return v < 1 ? DEFAULT_MAX_RECEIVER_THREAD_TIMERTOLIVE : v;

                } catch(Exception e) {

                    return DEFAULT_MAX_RECEIVER_THREADS;
                }
            }

            @Override
            public long getMaxReceiverThreadExecutions() {

                try {

                    final long v = inConfiguration.getMaxReceiverThreadExecutions();

                    return v < 1 ? DEFAULT_MAX_RECEIVER_THREAD_EXECUTIONS : v;

                } catch(Exception e) {

                    return DEFAULT_MAX_RECEIVER_THREAD_EXECUTIONS;
                }
            }
        };

    }

    private void log(Object inSrc, Object inMessage) {

        //String theMsg = "[" + (inSrc == null ? "?" : inSrc) + "] " + Thread.currentThread().getName() + "\n"+ inMessage;
        final String theMsg = "[" + (inSrc == null ? "?" : inSrc) + "] " + inMessage;

        final PrintStream theOut = configuration.getLogStream();
        if ( theOut != null ) {
            synchronized (theOut) {
                theOut.println( theMsg );
            }
        }
    }

    public int getCountMessagesInQueue() {
        synchronized (messages) {
            return messages.size();
        }
    }

    public void close() {

        isCloseSignal = true;
        
        _notify( this );
        
        log("QUEUE", "close announced");
    }

    public void addMessage(Serializable inMsg) {

        if (isCloseSignal) {
            throw new IllegalStateException("Queue is closed.");
        }

        if (receiverClass == null) {
            throw new IllegalStateException("no receiver class defined");
        }

        if (inMsg == null) {
            return;
        }

        insertItem(inMsg);
    }

    interface IReceiver {

        void handle(Serializable inMsg);
    }

    private class ReceiverThread extends Thread {

        public final long timeCreated = System.currentTimeMillis();
        public final long handlerThreadCount = ++InMemoryQueue.this.handlerThreadCount;

        long executionCounts = 0;

        boolean isKilled = false;
        IReceiver receiver;

        public ReceiverThread(IReceiver inReceicer) throws Exception {

            super.setName(InMemoryQueue.this.toString() + "," + handlerThreadCount + "," + timeCreated);

            receiver = inReceicer;

            super.setDaemon(false);

            super.start();
        }

        private Item fetchItem() {

            synchronized (messages) {

                return messages.isEmpty() ? null : messages.remove(0);
            }
        }

        void signalThreadKill() {
            isKilled = true;
            log("RT-" + handlerThreadCount, " received kill");
            _notify(this);
        }

        @Override
        public void run() {
        	
        	InMemoryQueue.this.countThreadsCreated++;

            Item item = null;

            while ( !isKilled ) {

                item = fetchItem();

                if ( isCloseSignal && item == null ) {

                    signalThreadKill();
                    continue;
                }

                if (item == null) {

                    log("RT-" + handlerThreadCount, " wait for message (messages=" + getStatus().getCountMessagesOnQueue() + ") ...");
                    _wait(this);

                    continue;
                }

                this.executionCounts++;

                try {

                    synchronized (ReceiverThread.class) {

                        log("RT-" + handlerThreadCount,
                                "handling item ... " + item + " ; handles=" + this.executionCounts);

                        item.start = System.currentTimeMillis();

                        receiver.handle(item.msg);
                    }

                } catch (Exception e) {

                    item.exception = e;

                } finally {

                    item.end = System.currentTimeMillis();
                    item = null;

                    final boolean isKill = isSubjectToKill();
                    if (isKill) {
                        
                    	signalThreadKill();
                    }
                    
                    if ( !isClosed && isCloseSignal && getStatus().getCountMessagesOnQueue() == 0 ) {
                    	
                    	isClosed = true;
                    	_notify( InMemoryQueue.this );
                    }

                    log("RT-" + handlerThreadCount, " done handling " + item + "; subject to kill=" + isKilled);
                }
            }

            rescheduleMessage( item );

            synchronized (InMemoryQueue.this.receiverThreadPool) {

                InMemoryQueue.this.receiverThreadPool.remove(this);
            }

            ensureCapacityReceiverThreads();

            log("RT-" + handlerThreadCount, "thread-run-end after " + executionCounts + " executions and "
                    + (System.currentTimeMillis() - timeCreated) + " ms liftime, message=" + item);
        }

        private boolean isSubjectToKill() {

            if (InMemoryQueue.this.isCloseSignal) {
                return true;
            }

            final long theMaxHandles = configuration.getMaxReceiverThreadExecutions();

            if (theMaxHandles < executionCounts) {
                log("RT-" + handlerThreadCount, " subject to kill due handle count");
                return true;
            }

            final long theTimeToLive = configuration.getMaxReceiverThreadTimeToLive();

            if (theTimeToLive > 0 && System.currentTimeMillis() > (this.timeCreated + theTimeToLive)) {

                log("RT-" + handlerThreadCount, " subject to kill due lifetime");
                return true;
            }

            return false;
        }
        
        @Override
        public String toString() {
        	return "ReceiverThread [handlerThreadCount="+ handlerThreadCount +", timeCreated="+ new Date( timeCreated ) +", isKilled="+ isKilled +", receiver="+ receiver +", parent-queue="+ InMemoryQueue.this.toString() +"]";
        }
    }

    private class Item implements Serializable {

        private static final long serialVersionUID = 4217360355156014416L;

        public final long creationTime = System.currentTimeMillis();
        public long start;
        public long end;
        public Exception exception;

        final Serializable msg;

        public Item(Serializable inMsg) {
            this.msg = inMsg;
        }

        @Override
        public String toString() {
            return "Item [creationTime=" + creationTime + ", start=" + start + ", end=" + end + ", exception="
                    + exception + ", msg=" + msg + "]";
        }
    }

    private IReceiver instantiateReceiver() throws InstantiationException, IllegalAccessException {

        return receiverClass.newInstance();
    }

    private void _wait(Object inObject) {

        synchronized (inObject) {

            try {

                long waitTimeout = configuration.getThreadWaitTimeout();
                waitTimeout = waitTimeout > 0 ? waitTimeout : 900;
                inObject.wait(waitTimeout);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void _notify(Object inObject) {
        synchronized (inObject) {
            inObject.notifyAll();
        }
    }

    private void insertItem(Serializable inMsg) {

        synchronized (messages) {

            final Item item = new Item(inMsg);
            messages.add(item);
            log("MAIN", "added message: " + item);

            ensureCapacityReceiverThreads();

            _notify(messages);
        }
    }

    private void rescheduleMessage(Item item) {

        if ( item == null ) { return; }

        synchronized (messages) {

            messages.add(0,item);
            log("MAIN", "re-added message: " + item);

            ensureCapacityReceiverThreads();

            _notify(messages);
        }
    }

    private void ensureCapacityReceiverThreads() {

        final int theConfiguredThreads = configuration.getMaxReceiverThreads();

        log("POOL","ensure-capacity ...; isCloseSignal="+ isCloseSignal +", threads="+ receiverThreadPool.size() +" messages="+ getCountMessagesInQueue());

        if ( isCloseSignal && getCountMessagesInQueue() < 1 ) {

            log("POOL","ensure-capacity skipped; isCloseSignal="+ isCloseSignal +", threads="+ receiverThreadPool.size() +" messages="+ getCountMessagesInQueue());
            return;
        }

        synchronized (receiverThreadPool) {

            final int theRtCount = receiverThreadPool.size();
            int adds = theConfiguredThreads - receiverThreadPool.size();
            adds = adds < 1 && receiverThreadPool.size() < 1 ? 1 : adds;

            log("POOL","ensure-capacity creating "+ adds +" (cnt.existing.threads="+ theRtCount +", maxThreads="+theConfiguredThreads+") receiver threads, isCloseSignal="+isCloseSignal +", threads="+ receiverThreadPool.size() +" messages="+ getCountMessagesInQueue());

            while (adds-- > 0) {
                try {

                    receiverThreadPool.add(new ReceiverThread(instantiateReceiver()));

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public interface IStatus {

        int getCountMessagesOnQueue();
        long getCountReceiverThreadsCreated();
    }

    public interface IConfiguration {

        int DEFAULT_MAX_RECEIVER_THREADS = 1;

        long DEFAULT_MAX_RECEIVER_THREAD_TIMERTOLIVE = 3 * 10L * 1000;
        long DEFAULT_MAX_RECEIVER_THREAD_EXECUTIONS = 100;

        /**
         * 10 seks
         */
        long DEFAULT_THREAD_WAIT_TIME = 10*1000;

        int getMaxReceiverThreads();

        long getThreadWaitTimeout();

        long getMaxReceiverThreadExecutions();

        long getMaxReceiverThreadTimeToLive();

        /**
         * when <code>false</code> the queue will be worked off by the handlers
         * @return
         */
        public boolean serializeMessagesOnClose();

        PrintStream getLogStream();
    }

	public void waitForClosed() {
		
		while( !this.isClosed ) {
			
			_wait( this );
		}
		
	}
}
