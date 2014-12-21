package edu.sjsu.cmpe273.CRDTClient;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Cache Client starts !!!");
        
        CRDTClient crdtClient = new CRDTClient();
        
		boolean reqStatus = crdtClient.put(1, "a");
        if (reqStatus) {        	
        	System.out.println("1st write complete - Thread sleeps for 30 counts !!");
        	Thread.sleep(30000);
        	reqStatus = crdtClient.put(1, "b");
        	if (reqStatus) {
        		System.out.println("2nd write complete - Thread sleeps for 30 counts !!");
            	Thread.sleep(30000);
            	String valueFromCrdtClient = crdtClient.get(1);
            	System.out.println("GET value from CRDT client "+valueFromCrdtClient);
        	} else {
            	System.out.println("Second write fails !!!");
        	}
        } else {
        	
			// failed
        	System.out.println("failed - First write !!!");
        }	
        System.out.println("Cache Client exited !!!");
        
    }

}
