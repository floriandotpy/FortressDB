package com.casualcoding.fortressdb;

public class ClientManager {

	public static void main(String[] args) {
		System.out.println("FortressDB:\n");
		
		
		Client first = new Client();
		first.beginTransaction();
		first.write(10, "first client - first transaction!");
		first.write(11, "first client - second transaction!");
		first.write(12, "first client - third transaction!");
		first.commit();
		
		Client second = new Client();
		second.beginTransaction();
		second.write(20, "second client - first transaction");
		second.write(21, "second client - second transaction");
		second.commit();
		
//		Client third = new Client();
//		third.beginTransaction();
//		third.write(30, "third client - first transaction");
//		third.write(31, "third client - second transaction");
		
	}
}
