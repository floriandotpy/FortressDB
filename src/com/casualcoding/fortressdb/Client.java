package com.casualcoding.fortressdb;

import java.util.LinkedList;
import java.util.List;

public class Client implements Runnable {
	
	private static final int BEGIN_TRANSACTION = 1;
	private static final int COMMIT_TRANSACTION = 2;
	private static final int WRITE = 3;
	
	private PersistanceManager _manager;
	private int _transactionId;
	private List<Object[]> _actions;
	
	public Client() {
		_manager = PersistanceManager.getPersistanceManager(); 
		_actions = new LinkedList<Object[]>();
	}
	
	public void beginTransaction() {
		_actions.add(new Object[]{BEGIN_TRANSACTION});
	}
	
	public void commit() {
		_actions.add(new Object[]{COMMIT_TRANSACTION});
	}
	
	public void write(int pageId, String data) {
		_actions.add(new Object[]{WRITE, pageId, data});
	}

	@Override
	public void run() {
		for(Object[] action : _actions) {
			switch ((Integer) action[0]) {
			case BEGIN_TRANSACTION:
				_transactionId = _manager.beginTransaction();
				break;
			case COMMIT_TRANSACTION:
				_manager.commit(_transactionId);
				_transactionId = -1;
				break;
			case WRITE:
				_manager.write(_transactionId, (Integer) action[1], (String) action[2]);
			}
			try {
				Thread.sleep(3000L);
			} catch (InterruptedException e) {
				// dafuq?
				e.printStackTrace();
			}
		}
	}
}
