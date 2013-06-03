package com.casualcoding.fortressdb;

public class Client {
	
	private PersistanceManager _manager;
	private int _transactionId;
	
	public Client() {
		_manager = PersistanceManager.getPersistanceManager(); 
	}
	
	public void beginTransaction() {
		_transactionId = _manager.beginTransaction();
	}
	
	public void commit() {
		_manager.commit(_transactionId);
		_transactionId = -1;
	}
	
	public void write(int pageId, String data) {
		_manager.write(_transactionId, pageId, data);
	}
}
