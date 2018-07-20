package de.greyshine.imqueue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class InMemoryQueue {

    private IConfiguration configuration;
    private List<Message> messages = new ArrayList<>();

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
        
    	this( new IConfiguration() {

			@Override
			public Class<? extends IReceiver> getReceiverClass() {
				return inReceiverClass;
			}
		} );
    }
    
    public InMemoryQueue(IConfiguration inConfiguration) {

    	if ( inConfiguration == null ) {
    		throw new IllegalArgumentException( "no configuration given" );
    	}
    	
        configuration = inConfiguration;

        receiverClass = inConfiguration.getReceiverClass();

        try {

            instantiateReceiver();

        } catch (Exception e) {

        	close();
            throw new IllegalArgumentException("Cannot instantiate Receiver: " + receiverClass, e);
        }
        
        if ( inConfiguration.serializeMessagesOnClose() ) {
        	throw new IllegalStateException( "serializing messages is not yet implemented" );
        }
        
        // read previous serialized files
        readSerializedMessages();
        
    }
    
    private void readSerializedMessages() {
		
    	final File theDirectory = configuration.getSerialisationDirectory();
    	
    	if ( !theDirectory.isDirectory() ) { return; }
    	
    	File[] theFiles = theDirectory.listFiles( (f)->{ return f.isFile(); } );
    	
    	for( File aFile : theFiles) {
    		
    		try ( ObjectInputStream theOis = new ObjectInputStream( new FileInputStream( aFile ) ) ) {
    			
    			Message theMessage = (Message) theOis.readObject();
    			
    			log("init", "deserialized: "+ theMessage);
    			
    			rescheduleMessage(theMessage);
    			
    		} catch(Exception e) {
    			
    			System.err.println( "failed to read / deserialze: "+ aFile+ ": "+e );
    		}
    	}
	}
    
    private void writeSerializedMessages() {
    	
    	final File theOutputFolder = configuration.getSerialisationDirectory().getAbsoluteFile();
    	
    	if ( !theOutputFolder.isDirectory() ) {
    		
    		theOutputFolder.mkdirs();
    	}

    	if ( !theOutputFolder.isDirectory() ) {
    		
    		throw new IllegalStateException("cannot access directory for serialization: "+ theOutputFolder);
    	}
    	
    	synchronized ( messages ) {
			
    		for( Message theMessage : new ArrayList<>(messages) ) {
    			
    			boolean isSuccessful = true;
    			
    			try (ObjectOutputStream oos = new ObjectOutputStream( new FileOutputStream( new File( theOutputFolder, UUID.randomUUID().toString()+".ser" ) ) ) ) {
					
    				oos.writeObject( theMessage );
    				
				} catch (Exception e) {
					
					isSuccessful = false;
					System.err.println( "failed to serialze message: "+ theMessage );
				} 
    			
    			if (isSuccessful) {
    				messages.remove( theMessage );
    			}
    		}
		}
    }

	public IStatus getStatus() {
    	return status;
    }

    private void log(Object inSrc, Object inMessage) {

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
        
        if ( configuration.serializeMessagesOnClose() ) {
        	writeSerializedMessages();
        }
        
        _notify( this );
        
        log("QUEUE", "close announced");
        
        waitForClosed();
    }

    public void addMessage(Serializable inData) {

        if (isCloseSignal) {
            throw new IllegalStateException("Queue is closed.");
        }

        if (inData == null) {
            return;
        }

        insertMessageData(inData);
    }
    
    private Message fetchNextMessage() {
    	
    	synchronized (messages) {
    		
    		if ( messages.isEmpty() ) { return null; }
    		
    		messages.sort( (i1,i2)->{ return i1.minSendTime.compareTo( i2.minSendTime ); } );
    		
    		final Message theMessage = messages.get(0);
    		
    		if ( theMessage.minSendTime > System.currentTimeMillis() ) {
    			
    			return null;
    		}
    		
    		messages.remove( theMessage );

            return theMessage;
        }
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

        void signalThreadKill() {
            isKilled = true;
            log("RT-" + handlerThreadCount, " received kill");
            _notify(this);
        }

        @Override
        public void run() {
        	
        	InMemoryQueue.this.countThreadsCreated++;

            Message theMessage = null;

            while ( !isKilled ) {

                theMessage = fetchNextMessage();

                if ( isCloseSignal && theMessage == null ) {

                    signalThreadKill();
                    continue;
                }

                if (theMessage == null) {

                    log("RT-" + handlerThreadCount, " wait for message (messages=" + getStatus().getCountMessagesOnQueue() + ") ...");
                    _wait(this);

                    continue;
                }

                this.executionCounts++;

                try {

                	receiverHandle(theMessage);

                } catch (Exception e) {
                	
                	theMessage.exceptions.add(e);
                	
                	if ( theMessage.exceptions.size() > configuration.getMaxReceiveAttempts() ) {

                		receiverHandleException( theMessage );
                	
                	} else {
                		
                		theMessage.minSendTime = System.currentTimeMillis() + configuration.getTimeBetweenReceiveAttempts();
                		
                		rescheduleMessage( theMessage );
                	}
                	
                } finally {

                    final boolean isKill = isSubjectToKill();

                    if (isKill) {
                    	
                    	signalThreadKill();
                    }
                    
                    if ( !isClosed && isCloseSignal && getStatus().getCountMessagesOnQueue() == 0 ) {
                    	
                    	isClosed = true;
                    	_notify( InMemoryQueue.this );
                    }

                    log("RT-" + handlerThreadCount, " done handling " + theMessage + "; subject to kill=" + isKilled);
                }
            }
            
            // receiver thread isKilled, let's tidy up

            synchronized (InMemoryQueue.this.receiverThreadPool) {

                InMemoryQueue.this.receiverThreadPool.remove(this);
            }

            ensureCapacityReceiverThreads();

            try {
			
            	// TODO add timeout for terminate invocation
            	receiver.terminate();

            } catch (Exception e) {
				
            	log("RT-" + handlerThreadCount, "receiver.terminate() failed: " + e);
			}
            
            log("RT-" + handlerThreadCount, "thread-run-end after " + executionCounts + " executions and "
                    + (System.currentTimeMillis() - timeCreated) + " ms liftime, message=" + theMessage);
        }

        private void receiverHandle(Message theMessage) {
        	
            synchronized (ReceiverThread.class) {

                log("RT-" + handlerThreadCount,
                        "handling item ... " + theMessage + " ; handles=" + this.executionCounts);

                theMessage.start = System.currentTimeMillis();
                theMessage.end = null;

                receiver.handle(theMessage.data);
                
                theMessage.end = System.currentTimeMillis();
                
            }
        }
        
        private void receiverHandleException(Message inMessage) {
			
        	synchronized (ReceiverThread.class) {

                log("RT-" + handlerThreadCount,
                        "handling message with exceptions ... " + inMessage + " ; handles=" + this.executionCounts);

                inMessage.start = System.currentTimeMillis();
                inMessage.end = null;

                try {
				
                	receiver.handleException( inMessage.exceptions , inMessage.data );
                	
				} catch (Exception e) {
				
					// intended swallow

				} finally {
					
					inMessage.end = System.currentTimeMillis();
				}
            }
		}

		private boolean isSubjectToKill() {

            if (InMemoryQueue.this.isCloseSignal) {
                return true;
            }

            final long theMaxHandles = defaultIfLess1(configuration.getMaxReceiverThreadExecutions(), IConfiguration.DEFAULT_MAX_RECEIVER_THREAD_EXECUTIONS);

            if (theMaxHandles < executionCounts) {
                log("RT-" + handlerThreadCount, " subject to kill due handle count");
                return true;
            }

            final long theTimeToLive = defaultIfLess1(configuration.getMaxReceiverThreadTimeToLive(), IConfiguration.DEFAULT_MAX_RECEIVER_THREAD_TIMERTOLIVE);

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

    private IReceiver instantiateReceiver() {

    	return configuration.getReceiverFactory( configuration.getReceiverClass() ).create();
    }

    private void _wait(Object inObject) {

        synchronized (inObject) {

            try {

                long waitTimeout = defaultIfLess1(configuration.getThreadWaitTimeout(), IConfiguration.DEFAULT_THREAD_WAIT_TIME);
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

    private void insertMessageData(Serializable inData) {

        synchronized (messages) {

            final Message item = new Message(inData);
            messages.add(item);
            log("MAIN", "added message: " + item);

            ensureCapacityReceiverThreads();

            _notify(messages);
        }
    }

    private void rescheduleMessage(Message inMessage) {

        if ( inMessage == null ) { return; }

        synchronized (messages) {

        	if ( !messages.contains( inMessage ) ) {
        		
        		// if block must always happen!
        		messages.add(inMessage);
        	}
        	
            log("MAIN", "re-added message: " + inMessage);

            ensureCapacityReceiverThreads();

            _notify(messages);
        }
    }

    private void ensureCapacityReceiverThreads() {

        final int theConfiguredThreads = defaultIfLess1(configuration.getMaxReceiverThreads(), IConfiguration.DEFAULT_MAX_RECEIVER_THREADS);

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

	public void waitForClosed() {
		while( !this.isClosed ) {
			_wait( this );
		}
	}

	private static int defaultIfLess1(int v, int d) {
		return v < 1 ? d : v;
	}
	private static long defaultIfLess1(long v, long d) {
		return v < 1 ? d : v;
	}
}
