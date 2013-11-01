package com.localxdata.test;

import com.localxdata.storage.StorageNozzle;
import com.localxdata.util.LogUtil;

public class Main {
    public static void main(String[] args) {
        
    	LogUtil.PRINTMEM("main", "main start");
    	
        StorageNozzle.initStorage();

        Test_ExcuteSql_Int excuteSqlInt = new Test_ExcuteSql_Int();
        excuteSqlInt.startTest();
        
    	
    	//Test_Class_DataCellList t = new Test_Class_DataCellList();
    	//t.startTest();
        
        Thread t = new Thread(new Runnable(){

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while(true) {
				    System.out.println("memory is " + java.lang.Runtime.getRuntime().totalMemory());
				    try {
						Thread.sleep(1000*10);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
        	
        });
        
        t.start();
    }
}