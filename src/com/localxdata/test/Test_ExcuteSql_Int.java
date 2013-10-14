package com.localxdata.test;

import com.localxdata.sql.ExcuteSqlBySingleTable;
import com.localxdata.storage.DataCellList;

import java.io.PrintStream;
import java.util.ArrayList;

public class Test_ExcuteSql_Int implements TestInf {
    private ExcuteSqlBySingleTable sqlExcute = ExcuteSqlBySingleTable
            .getInstance();
    
    private static final int TEST_RECORDS = 5000;

    public void Test_create_Data(int times) {
    	ArrayList<Object>listD = getDatalist();   

    	int size = 0;
    	
    	if(listD != null) {
    		size += listD.size();
    	}
    	
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
        
        //check
    }

    public boolean Test_Query_Data_Equal_1() {
    	String str = "data3 == 100";
    	
        System.out.println(getClass() + ":Test_Query_Data_Equal start at "
                + System.currentTimeMillis());
        ArrayList list = this.sqlExcute.query(Test_Class_SmallData_Int.class
                .getName(), str);
        System.out.println(getClass() + ":Test_Query_Data_Equal end at "
                + System.currentTimeMillis());

        System.out.println("list size is " + list.size());
        if(list.size() == 1) {
        	return true;
        }
        return false;
    }
    
    public boolean Test_Query_Data_Equal_2() {
    	String str = "data1 == 100 || data2 == 101";
    	ArrayList list = this.sqlExcute.query(Test_Class_SmallData_Int.class
                .getName(), str);
    	
        if(list.size() == 2) {
        	return true;
        }
        
        return false;
    	
    }
    
    public boolean Test_Query_Data_Equal_3() {
    	String str = "data1 == 100 && data2 == 101";
    	ArrayList list = this.sqlExcute.query(Test_Class_SmallData_Int.class
                .getName(), str);
    	
        if(list.size() == 0) {
        	return true;
        }
        
        return false;
    }
    
    
    public boolean Test_Query_Data_Not_Equal_1() {
    	String str = "data1 != 100";
    	
        System.out.println(getClass() + ":Test_Query_Data_Not_Equal start at "
                + System.currentTimeMillis());
        ArrayList list = this.sqlExcute.query(Test_Class_SmallData_Int.class
                .getName(), str);
        System.out.println(getClass() + ":Test_Query_Data_Not_Equal end at "
                + System.currentTimeMillis());

        if(list.size() == TEST_RECORDS - 1) {
        	return true;
        }
        return false;
    }
    
    public boolean Test_Query_Data_Update() {
    	//check update
    	ArrayList<Object>list = getDatalist();   
    	boolean isTestDataExist = false;
        for(Object obj:list) {
        	Test_Class_SmallData_Int d = (Test_Class_SmallData_Int)obj;
         	
        	if(d.data3 == 100 && d.data1 != -1) {
        		isTestDataExist = true;
        		break;
            }
        }
        
        if(!isTestDataExist) {
        	System.out.println(getClass() + "Test_Query_Data_Update no test data!!!");
        	return false;
        }
    	
    	Test_Class_SmallData_Int data = new Test_Class_SmallData_Int();
    	data.data1 = -1;
    	
    	String arg[] ={"data1"};
    	this.sqlExcute.update(data,arg,"data3 == 100");
    	
    	list = getDatalist();
        for(Object obj:list) {
        	Test_Class_SmallData_Int d = (Test_Class_SmallData_Int)obj;
         	
        	if(d.data3 == 100 && d.data1 != -1) {
        		return false;
            }
        }
        
        return true;
    }
    
    public boolean Test_Query_Data_Delete() {
    	
    	ArrayList<Object>list = getDatalist();   
    	boolean isTestDataExist = false;
        for(Object obj:list) {
        	Test_Class_SmallData_Int d = (Test_Class_SmallData_Int)obj;
         	
        	if(d.data3 == 1) {
        		isTestDataExist = true;
        		break;
            }
        }
        
        if(!isTestDataExist) {
        	System.out.println(getClass() + "Test_Query_Data_Delete no test data!!!");
        	return false;
        }
        
    	Test_Class_SmallData_Int data = new Test_Class_SmallData_Int();
    	
    	this.sqlExcute.delete(Test_Class_SmallData_Int.class.getName(), 
    			              "data3 == 1");
    	
    	//check
    	list = getDatalist();
        for(Object obj:list) {
        	Test_Class_SmallData_Int d = (Test_Class_SmallData_Int)obj;
         	
        	if(d.data3 == 1) {
        		return false;
            }
        }
        
        return true;
    }
    
    public boolean Test_Data_LessThan_1() {
    	ArrayList<Object> list = this.sqlExcute.query(Test_Class_SmallData_Int.class.getName(), "data3 < 100");
    	
    	if(list.size() == 100) {
    		return true;
    	}
    	
    	return false;
    }

    
    public boolean Test_Data_LessThan_2() {
        ArrayList<Object> list = this.sqlExcute.query(Test_Class_SmallData_Int.class.getName(), "data3 <= 100");
    	
    	if(list.size() == 101) {
    		return true;
    	}
    	
    	return false;
    }
    
    
    public boolean Test_Data_LessThan_3() {
    	ArrayList<Object> list = this.sqlExcute.query(Test_Class_SmallData_Int.class.getName(), "data3 < 100 && data3 > 50");
    	
    	if(list.size() == 49) {
    		return true;
    	}
    	
    	return false;
    }
    
    public boolean Test_Data_MoreThan_1() {
    	ArrayList<Object> list = this.sqlExcute.query(Test_Class_SmallData_Int.class.getName(), "data3 >" + (TEST_RECORDS-100));
    	
        if(list.size() == 99) {
        	return true;
        }	
        
        return false;
    }

    
    public boolean Test_Data_MoreThan_2() {
        ArrayList<Object> list = this.sqlExcute.query(Test_Class_SmallData_Int.class.getName(), "data3 >=" + (TEST_RECORDS-100));
    	
        if(list.size() == 100) {
        	return true;
        }	
        
        return false;
    }
    
    public boolean Test_Data_MoreThan_3() {
       return false;
    }
    

    public int Test_Index_Create() {
        return 1;
    }

    public void startTest() {
    	System.out.println("-------------start Test_ExcuteSql_Int--------------- ");
    	
        //Test_create_Data(TEST_RECORDS);
        
        if(Test_Query_Data_Equal_1()) {
            System.out.println(getClass() + ":Test_Query_Data_Equal_1 OK");	
        }else {
        	System.out.println(getClass() + ":Test_Query_Data_Equal_1 Fail");
        }
        
        if(Test_Query_Data_Equal_2()) {
        	System.out.println(getClass() + ":Test_Query_Data_Equal_2 OK");
        }else {
        	System.out.println(getClass() + ":Test_Query_Data_Equal_2 Fail");
        }
        
        if(Test_Query_Data_Equal_3()) {
        	System.out.println(getClass() + ":Test_Query_Data_Equal_3 OK");
        }else {
        	System.out.println(getClass() + ":Test_Query_Data_Equal_3 Fail");
        }
        
        if(Test_Query_Data_Not_Equal_1()) {
            System.out.println(getClass() + ":Test_Query_Data_Not_Equal_1 OK");	
        }else {
        	System.out.println(getClass() + ":Test_Query_Data_Not_Equal_1 Fail");
        }
        
        if(Test_Data_LessThan_1()) {
        	System.out.println(getClass() + ":Test_Data_LessThan_1 OK");
        }else {
        	System.out.println(getClass() + ":Test_Data_LessThan_1 Fail");
        }
        
        if(Test_Data_LessThan_2()) {
        	System.out.println(getClass() + ":Test_Data_LessThan_2 OK");
        }else {
        	System.out.println(getClass() + ":Test_Data_LessThan_2 Fail");
        }
        
        if(Test_Data_LessThan_3()) {
        	System.out.println(getClass() + ":Test_Data_LessThan_3 OK");
        }else {
        	System.out.println(getClass() + ":Test_Data_LessThan_3 Fail");
        }
        
        if(Test_Data_MoreThan_1()) {
        	System.out.println(getClass() + ":Test_Data_MoreThan_1 OK");
        }else {
        	System.out.println(getClass() + ":Test_Data_MoreThan_1 Fail");
        }
        
        if(Test_Data_MoreThan_2()) {
        	System.out.println(getClass() + ":Test_Data_MoreThan_2 OK");
        }else {
        	System.out.println(getClass() + ":Test_Data_MoreThan_2 Fail");
        }
        
        //if(Test_Query_Data_Update()) {
        //    System.out.println(getClass() + ":Test_Query_Data_Update OK");
        //}else {
        //	System.out.println(getClass() + ":Test_Query_Data_Update Fail");
        //}
        
        //if(Test_Query_Data_Delete()) {
        //    System.out.println(getClass() + ":Test_Query_Data_Delete OK");
        //}else {
        //	System.out.println(getClass() + ":Test_Query_Data_Delete Fail");
        //}
        
        System.out.println("-------------end Test_ExcuteSql_Int--------------- ");
    }
    
    
    private ArrayList<Object>getDatalist() {
    	return this.sqlExcute.query(Test_Class_SmallData_Int.class.getName());
    }
}