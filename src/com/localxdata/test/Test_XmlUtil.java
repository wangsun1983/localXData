package com.localxdata.test;

import com.localxdata.storage.DataCellList;
import com.localxdata.struct.DataCell;
import com.localxdata.util.XmlUtil;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;

public class Test_XmlUtil implements TestInf {

    @Override
    public void startTest() {
        // TODO Auto-generated method stub
        
    }
    
    /*
    private boolean testCreateByFileName(String filename) {
        XmlUtil x = XmlUtil.getInstance();
        if (x.CreateDataXml(filename) != XmlUtil.RESULT_CREATE_SUCCESS) {
            return false;
        }

        return true;
    }

    private boolean testCreateByClass() {
        XmlUtil x = XmlUtil.getInstance();

        Test_Class_Student aa1 = new Test_Class_Student();
        aa1.init("tome1", 1);

        Test_Class_Student aa2 = new Test_Class_Student();
        aa2.init("tome2", 2);

        Test_Class_Student aa3 = new Test_Class_Student();
        aa3.init("tome3", 3);

        Test_Class_Student aa4 = new Test_Class_Student();
        aa4.init("tome4", 4);

        ArrayList list = new ArrayList();

        for (int i = 0; i < 10; i++) {
            Test_Class_Student aa = new Test_Class_Student();
            aa.init("tome" + i, i);
            list.add(aa);
        }

        if (x.CreateXml(list) != XmlUtil.RESULT_CREATE_SUCCESS) {
            return false;
        }

        return true;
    }

    public boolean testLoadXml_ex() {
        XmlUtil x = XmlUtil.getInstance();

        System.out.println("testLoadXml_ex start at "
                + System.currentTimeMillis());
        ArrayList list = x.LoadXml_Ex(Test_Class_SmallData_String.class
                .getName());
        System.out.println("testLoadXml_ex end at "
                + System.currentTimeMillis());
        System.out.println("testLoadXml,list count is " + list.size());

        return true;
    }

    public boolean testLoadXml_sax() {
        XmlUtil x = XmlUtil.getInstance();

        System.out.println("testLoadXml_sax start at "
                + System.currentTimeMillis());
        ArrayList<DataCell> list = x.LoadDataXml_Sax(Test_Class_SmallData_String.class
                .getName());
        System.out.println("testLoadXml_sax end at "
                + System.currentTimeMillis());
        System.out.println("testLoadXml_sax,list count is " + list.size());

        return true;
    }

    public boolean testLoadXml_All() {
        XmlUtil x = XmlUtil.getInstance();

        System.out.println("testLoadXml_All start at "
                + System.currentTimeMillis());
        
        HashMap<String, DataCellList> dataListMap = new HashMap<String, DataCellList>();
        x.LoadAllDataXml(dataListMap);
        System.out.println("testLoadXml_All end at "
                + System.currentTimeMillis());

        return true;
    }

    public void startTest() {
        System.out.println("Test_XmlUtil start test");

        testLoadXml_All();
    }
    */
}