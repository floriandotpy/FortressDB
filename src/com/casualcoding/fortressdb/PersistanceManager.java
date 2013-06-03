package com.casualcoding.fortressdb;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

public class PersistanceManager {
	private static final int MAX_BUFFER_SIZE = 5;
	private static final String DB_FOLDER = "db/";
	
	private static PersistanceManager _pmngr;
	private Map<Integer, Object[]> _buffer; // pageId -> (transactionId, data)

	private Map<Integer, Object[]> _committedTransactions;
	private int _currentTransactionId;
	private int _logSequenceNumber;

	
	private PersistanceManager() {
		_buffer = new HashMap<Integer, Object[]>();
		_currentTransactionId = 0;
		_logSequenceNumber = 0;
		_committedTransactions = new HashMap<Integer, Object[]>();
	}
	
	public synchronized int beginTransaction() {
		_currentTransactionId++;
		return _currentTransactionId;
	}
	
	public void commit(int transactionId) {

		for(Entry<Integer, Object[]> entry: _buffer.entrySet()) {
			
			Object[] tuple = entry.getValue();
			if ((int)tuple[0] == transactionId) {
				
				int logSequenceNumber = log(entry.getKey(), (int)tuple[0], (String)tuple[1]);
				
				tuple[2] = logSequenceNumber;
				_committedTransactions.put(entry.getKey(), tuple);
			}
		}
		for (Integer key: _committedTransactions.keySet()) {
			_buffer.remove(key);
		}
	}
	
	public void write(int transactionId, int pageId, String data) {
		
		_buffer.put(pageId, new Object[]{ transactionId, data,});
		if (_buffer.size()+_committedTransactions.size() > MAX_BUFFER_SIZE) {
			persistTransactions();
		}
	}
	
	private int log(int pageId, int transactionId, String data) {
		int logSequenceNumber = getLogSequenceNumber();
		String filename = DB_FOLDER + String.format("lsn-%03d.log", logSequenceNumber);
		String filecontent = String.format("%d,%d,%d,%s", logSequenceNumber, transactionId, pageId, data);
		FileWriter file;
		try {
			file = new FileWriter(filename);
			file.write(filecontent);
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return logSequenceNumber;
	}

	private synchronized int getLogSequenceNumber() {
		_logSequenceNumber++;
		return _logSequenceNumber;
	}

	private void persistTransactions() {
		for(Entry<Integer, Object[]> entry: _committedTransactions.entrySet()) {
			// write entry
			Object[] tuple = entry.getValue();
			int pageId = entry.getKey();
			String filename = DB_FOLDER + String.format("page-%03d.page", pageId);
			String filecontent = String.format("%03d,%03d,%s", pageId, tuple[2], tuple[1]);
			FileWriter file;
			try {
				file = new FileWriter(filename);
				file.write(filecontent);
				file.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
