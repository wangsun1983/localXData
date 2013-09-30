package com.localxdata.test;

import com.localxdata.storage.StorageNozzle;

public class Main {
    public static void main(String[] args) {
        
        StorageNozzle.initStorage();

        Test_ExcuteSql_Int excuteSqlInt = new Test_ExcuteSql_Int();
        excuteSqlInt.startTest();
        
    	
    	//Test_Class_DataCellList t = new Test_Class_DataCellList();
    	//t.startTest();
        
    }
}