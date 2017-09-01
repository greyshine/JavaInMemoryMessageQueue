package de.greyshine.imqueue;

public class DefaultReceiverFactory<T extends IReceiver> implements IReceiverFactory<T> {

	private final Class<T> clazz;
	
	public DefaultReceiverFactory(Class<T> clazz) {
		
		this.clazz = clazz;
		
		// test if an instance can be created
		
		create().terminate();
		
	}

	@Override
	public T create() {
		
		try {
			
			return clazz.newInstance();
		
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException();
		}
	}

}
