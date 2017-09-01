package de.greyshine.imqueue;

/**
 * Factory for creating new receiver instances handling incoming messages 
 * 
 * @param <T>
 */
public interface IReceiverFactory<T extends IReceiver> {
	
	T create();
	
}
