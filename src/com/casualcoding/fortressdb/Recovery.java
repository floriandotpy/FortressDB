package com.casualcoding.fortressdb;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Recovery {
	
	// transactionId -> list(tuples(lsn, pageId, data))
	private	Map<Integer, List<Object[]>> _log;
	
	// pageId -> lsn
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
				
				// check if data has never been persisted OR data is missing a write
				if(pageLogSequenceNumber == null || pageLogSequenceNumber < logSequenceNumber) {
					winnerTransactions.add(entry.getKey());
					System.out.println(String.format("Not persisted winner transaction: %03d", entry.getKey()));
					break; // no need to check further, we'll have to write the whole transaction anyway
				}
			}
		}
		
		
		if(winnerTransactions.size() > 0) {
			for(Integer transactionId: winnerTransactions) {
				System.out.println(String.format("Redo transaction: %03d", transactionId));
				for(Object[] log: _log.get(transactionId)) {
					
					int lsn = (int) log[0];
					int pageId = (int) log[1];
					String data = (String) log[2];
					String filename = path + String.format("page-%03d.page", pageId);
					String filecontent = String.format("%d,%d,%s", pageId, lsn, data);
					Tools.writeFile(filename, filecontent);
					
				}
			}

			removeLogFiles(path);			
		}
	}

	private void removeLogFiles(String path) {
		File folder = new File(path);
		for (File file : folder.listFiles()) {
			if (file.isFile()) {
				String filename = file.getName();
				if (filename.endsWith(".log")) {
					file.delete();
				} 
			}
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
		String[] filecontent = Tools.readFile(file);
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
		String[] filecontent = Tools.readFile(file);
		if (filecontent.length >= 2) {
			int pageId = Integer.valueOf(filecontent[0]);
			int logSequenceNumber = Integer.valueOf(filecontent[1]);
			_pages.put(pageId,logSequenceNumber);			
			System.out.println(String.format("Page: id -> %03d, lsn -> %03d", pageId, logSequenceNumber));
		}
	}


	public static void main(String[] args) {
		Recovery recovery = new Recovery();
		recovery.startRecovery(PersistanceManager.DB_FOLDER);
	}

}
