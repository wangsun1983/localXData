package com.localxdata.test;

import com.localxdata.sql.ExcuteSqlBySingleTable;
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
            if (i % 10000 == 1) {
                System.gc();
            }
        }
        this.sqlExcute.creatTable(Test_Class_SmallData_String.class.getName(),
                list);
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

        return list.size();
    }

    public int Test_Data_LessThan(String str) {
        return -1;
    }

    public int Test_Index_Create() {
        return 1;
    }

    public void startTest() {
        Test_create_Data(500);

        int result = Test_Query_Data_Equal("data1 < 100 && data2 < 100");
        if (result == 1)
            System.out.println(getClass() + "Test_Query_Data_Equal trace1 OK");
        else
            System.out.println(getClass() + "Test_Query_Data_Equal trace1 NG");
    }
}