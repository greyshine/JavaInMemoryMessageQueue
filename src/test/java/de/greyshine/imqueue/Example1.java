package de.greyshine.imqueue;

import java.io.PrintStream;
import java.io.Serializable;

import org.junit.Assert;
import org.junit.Test;

import de.greyshine.imqueue.InMemoryQueue;

public class Example1 {
	
	private static long receiverThreadSleepTime = 300;
	
	public static IConfiguration CONFIGURATION = new IConfiguration() {

        @Override
        public int getMaxReceiverThreads() {
            return 2;
        }

        @Override
        public long getThreadWaitTimeout() {
            return 0;
        }

        @Override
        public long getMaxReceiverThreadExecutions() {
            return 2;
        }

        @Override
        public long getMaxReceiverThreadTimeToLive() {
            return 0;
        }

        @Override
        public boolean serializeMessagesOnClose() {
            return false;
        }

        @Override
        public PrintStream getLogStream() {
            return System.out;
        }

		@Override
		public Class<? extends IReceiver> getReceiverClass() {
			return TestReceiver.class;
		}
    };

	@Test
	public void test() throws InterruptedException {
		
		InMemoryQueue imq = new InMemoryQueue( CONFIGURATION );

        final long theThreadSleepTime = 101;
        final int theAmountMsgs = 5;

        for (int cnt = 0; cnt < theAmountMsgs; cnt++) {

            imq.addMessage("#MSG#" + (cnt + 1));
            Thread.sleep(theThreadSleepTime);
        }

        imq.close();

        try {

            imq.addMessage("#MSG#" + (theAmountMsgs + 1));

            System.err.println("must not happen");
            System.exit(1);

        } catch (Exception e) {
        	
        	System.out.println( "[MAIN] finish: " + imq.getCountMessagesInQueue());
        }
        
        imq.waitForClosed();
        
        Assert.assertEquals( 0, imq.getStatus().getCountMessagesOnQueue() );
        Assert.assertEquals( 5 ,imq.getStatus().getCountReceiverThreadsCreated() );
        Assert.assertEquals( theAmountMsgs , TestReceiver.handles);
	}
	
    public static class TestReceiver implements IReceiver {

        static volatile int handles = 0;

        public void handle(Serializable inMsg) {

            try {

                Thread.sleep( receiverThreadSleepTime );

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("HANDLERING:-) "+ inMsg + " (all=" + (++handles) + ") >> "+Thread.currentThread());
        }
    }
	
}
