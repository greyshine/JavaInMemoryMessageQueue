package de.greyshine.imqueue;

public interface IStatus {
	
	int getCountMessagesOnQueue();
    long getCountReceiverThreadsCreated();

}
