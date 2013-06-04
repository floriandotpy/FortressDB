package com.casualcoding.fortressdb;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class PersistanceManager {
	public static final String DB_FOLDER = "db/";

	private static final int MAX_BUFFER_SIZE = 5;
	
	private static PersistanceManager _pmngr;
	private Map<Integer, Object[]> _buffer; // pageId -> (transactionId, data)
	private Map<Integer, Object[]> _committedTransactions;
	private File _currentTransaction;
	private File _currentLogSequenceNumber;
	
	private PersistanceManager() {
		_buffer = new HashMap<Integer, Object[]>();
		_committedTransactions = new HashMap<Integer, Object[]>();
		_currentTransaction = new File(DB_FOLDER + "transaction.current");
		_currentLogSequenceNumber = new File(DB_FOLDER + "lsn.current");
	}
	
	public synchronized int beginTransaction() {
		int currentTransactionId;
		String saved = Tools.readFile(_currentTransaction)[0];
		if(saved != "") {
			currentTransactionId = Integer.valueOf(saved);
		} else {
			currentTransactionId = 0;
		}
		currentTransactionId++;
		Tools.writeFile(_currentTransaction, "" + currentTransactionId);
		return currentTransactionId;
	}
	
	public synchronized void commit(int transactionId) {
		System.out.println(String.format("commit transaction %03d.\n", transactionId));
		for(Entry<Integer, Object[]> entry: _buffer.entrySet()) {
			
			Object[] tuple = entry.getValue();
			if ((Integer) tuple[0] == transactionId) {
				
				int logSequenceNumber = log(entry.getKey(), (Integer) tuple[0], (String)tuple[1]);
				
				tuple = new Object[] {tuple[0], tuple[1], logSequenceNumber};
				_committedTransactions.put(entry.getKey(), tuple);
			}
		}
		for (Integer key: _committedTransactions.keySet()) {
			_buffer.remove(key);
		}
	}
	
	public synchronized void write(int transactionId, int pageId, String data) {
		
		
		System.out.println(String.format("Write to buffer: pageId -> %03d, transactionId -> %03d, data -> %s", pageId, transactionId, data));
		_buffer.put(pageId, new Object[]{ transactionId, data});
		
		System.out.println(String.format("Buffer size: %d\n", _buffer.size() + _committedTransactions.size()));
		if (_buffer.size()+_committedTransactions.size() > MAX_BUFFER_SIZE) {
			persistTransactions();
		} else {
		}
	}
	
	private int log(int pageId, int transactionId, String data) {
		int logSequenceNumber = getLogSequenceNumber();
		String filename = DB_FOLDER + String.format("lsn-%03d.log", logSequenceNumber);
		String filecontent = String.format("%d,%d,%d,%s", logSequenceNumber, transactionId, pageId, data);
		Tools.writeFile(filename, filecontent);
		
		return logSequenceNumber;
	}

	private synchronized int getLogSequenceNumber() {
		int currentLogSequenceNumber; 
		String saved = Tools.readFile(_currentLogSequenceNumber)[0];
		if (saved != "") {
			currentLogSequenceNumber = Integer.valueOf(saved);
		} else {
			currentLogSequenceNumber = 0;
		}
		currentLogSequenceNumber++;
		Tools.writeFile(_currentLogSequenceNumber, "" + currentLogSequenceNumber);
		return currentLogSequenceNumber;
	}

	private void persistTransactions() {
		System.out.println("Write commited transactions to persistent storage.\n");
		for(Entry<Integer, Object[]> entry: _committedTransactions.entrySet()) {
			// write entry
			Object[] tuple = entry.getValue();
			int pageId = entry.getKey();
			String filename = DB_FOLDER + String.format("page-%03d.page", pageId);
			String filecontent = String.format("%d,%d,%s", pageId, tuple[2], tuple[1]);
			Tools.writeFile(filename, filecontent);
		}
		
		_committedTransactions.clear();

	}
	
	public static PersistanceManager getPersistanceManager() {
		if (_pmngr == null) {
			_pmngr = new PersistanceManager();
		}
		return _pmngr;
	}	

}
