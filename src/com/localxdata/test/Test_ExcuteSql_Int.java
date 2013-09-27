package com.localxdata.test;

import com.localxdata.sql.ExcuteSqlBySingleTable;
import com.localxdata.storage.DataCellList;

import java.io.PrintStream;
import java.util.ArrayList;

public class Test_ExcuteSql_Int implements TestInf {
    private ExcuteSqlBySingleTable sqlExcute = ExcuteSqlBySingleTable
            .getInstance();

    public void Test_create_Data(int times) {
        System.out.println(getClass() + ":Test_create start at "
                + System.currentTimeMillis());
        ArrayList list = new ArrayList();
        for (int i = 0; i < times; i++) {
            Test_Class_SmallData_Int testData = new Test_Class_SmallData_Int();
            testData.init(i);
            list.add(testData);
        }
        this.sqlExcute.insert(list);
        System.out.println(getClass() + ":Test_create end at "
                + System.currentTimeMillis());
    }

    public int Test_Query_Data_Equal(String str) {
        System.out.println(getClass() + ":Test_Query_Data_Equal start at "
                + System.currentTimeMillis());
        ArrayList list = this.sqlExcute.query(Test_Class_SmallData_Int.class
                .getName(), str);
        System.out.println(getClass() + ":Test_Query_Data_Equal end at "
                + System.currentTimeMillis());

        for(Object data:list) {
            System.out.println("Test_Query_Data_Equal is " + data);
            Test_Class_SmallData_Int testData = (Test_Class_SmallData_Int)data;
            System.out.println("testData " + testData.data1);
        }
        return list.size();
    }
    
    public void Test_Query_Data_Update() {
    	Test_Class_SmallData_Int data = new Test_Class_SmallData_Int();
    	data.data1 = -1;
    	
    	String arg[] ={"data1"};
    	this.sqlExcute.update(data,arg,"data3 == 100");
    }
    
    public void Test_Query_Data_Delete() {
    	Test_Class_SmallData_Int data = new Test_Class_SmallData_Int();
    	
    	this.sqlExcute.delete(Test_Class_SmallData_Int.class.getName(), 
    			              "data3 == 1");
    }

    public int Test_Data_LessThan(String str) {
        return -1;
    }

    public int Test_Index_Create() {
        return 1;
    }

    public void startTest() {
        Test_create_Data(5000);
        //Test_Query_Data_Update();
        
        //Test_Query_Data_Delete();
        
        int result = Test_Query_Data_Equal("data1 == 100");
        System.out.println("result is " + result);
        if (result == 1)
            System.out.println(getClass() + "Test_Query_Data_Equal trace1 OK");
        else
            System.out.println(getClass() + "Test_Query_Data_Equal trace1 NG");
    }
}