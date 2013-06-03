package com.casualcoding.fortressdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Recovery {
	
	private	Map<Integer, List<Object[]>> _log;
	private Map<Integer, Integer> _pages;
	
	public Recovery() {
		_log = new HashMap<Integer, List<Object[]>>();
		_pages = new HashMap<Integer, Integer>();
	}
	
	public void startRecovery(String path) {
		loadFiles(path);
		
		List<Integer> winnerTransactions = new LinkedList<Integer>();
		
		for(Entry<Integer, List<Object[]>> entry: _log.entrySet()) {
			for(Object[] log: entry.getValue()) {
				int logSequenceNumber = (Integer) log[0];
				int pageId = (Integer) log[1];
				Integer pageLogSequenceNumber = _pages.get(pageId);
				if(pageLogSequenceNumber == null || pageLogSequenceNumber < logSequenceNumber) {
					winnerTransactions.add(entry.getKey());
					System.out.println(String.format("Not persisted winner transaction: %03d", entry.getKey()));
					break;
				}
			}
		}
		
		if(winnerTransactions.size() > 0) {
			PersistanceManager manager = PersistanceManager.getPersistanceManager();
			int tid = manager.beginTransaction();
			for(Integer transactionId: winnerTransactions) {
				System.out.println(String.format("Redo transaction: %03d", transactionId));
				for(Object[] log: _log.get(transactionId)) {
					manager.write(tid, (Integer) log[1], (String) log[2]); 
				}
			}
			manager.commit(tid);
		}
	}
	
	private void loadFiles(String path) {
		File folder = new File(path);
		for (File file : folder.listFiles()) {
			if (file.isFile()) {
				String filename = file.getName();
				if (filename.endsWith(".log")) {
					addLog(file);
				} else if (filename.endsWith(".page")) {
					addPage(file);
				}
			}
		}
	}
	
	private void addLog(File file) {
		String[] filecontent = readFile(file);
		if (filecontent.length >= 4) {
			int logSequenceNumber = Integer.valueOf(filecontent[0]);
			int transactionId = Integer.valueOf(filecontent[1]);
			int pageId = Integer.valueOf(filecontent[2]);
			String data = filecontent[3];
			
			if(!_log.containsKey(transactionId)) {
				_log.put(transactionId, new LinkedList<Object[]>());
			}			
			_log.get(transactionId).add(new Object[] {logSequenceNumber, pageId, data});			
			System.out.println(String.format("Log: lsn -> %03d, transactionId -> %03d, page -> %03d",
					logSequenceNumber, transactionId, pageId));
		}
	}
	
	private void addPage(File file) {
		String[] filecontent = readFile(file);
		if (filecontent.length >= 2) {
			int pageId = Integer.valueOf(filecontent[0]);
			int logSequenceNumber = Integer.valueOf(filecontent[1]);
			_pages.put(pageId,logSequenceNumber);			
			System.out.println(String.format("Page: id -> %03d, lsn -> %03d", pageId, logSequenceNumber));
		}
	}
	
	public static String[] readFile(File file) {
		String line = "";
		try {
			if (file.exists()) {
				BufferedReader reader = new BufferedReader(new FileReader(file));
				line = reader.readLine();
				reader.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return line.split(",");
	}

	public static void main(String[] args) {
		Recovery recovery = new Recovery();
		recovery.startRecovery(PersistanceManager.DB_FOLDER);
	}

}
