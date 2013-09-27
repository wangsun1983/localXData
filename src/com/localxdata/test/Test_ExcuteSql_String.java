package com.localxdata.test;

import com.localxdata.sql.ExcuteSqlBySingleTable;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;

public class Test_ExcuteSql_String implements TestInf {
    private ExcuteSqlBySingleTable sqlExcute = ExcuteSqlBySingleTable
            .getInstance();

    public void Test_excute() {
        Long s = Long.valueOf(System.currentTimeMillis());
        ArrayList list = this.sqlExcute.query(Test_Class_Student.class
                .getName(), " age == 2 && name LIKE 'tome%'");

        for (Iterator localIterator = list.iterator(); localIterator.hasNext();) {
            Object obj = localIterator.next();
            Test_Class_Student t = (Test_Class_Student) obj;
            System.out.println("1 Test_excute t.age = " + t.age);
            System.out.println("1 Test_excute name = " + t.name);
        }
        System.out.println(System.currentTimeMillis() - s.longValue());
    }

    public void Test_create_Data(int times) {
        System.out.println(getClass() + ":Test_create start at "
                + System.currentTimeMillis());
        ArrayList list = new ArrayList();
        for (int i = 0; i < times; i++) {
            Test_Class_SmallData_String testData = new Test_Class_SmallData_String();
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
        ArrayList list = this.sqlExcute.query(Test_Class_SmallData_String.class
                .getName(), str);
        System.out.println(getClass() + ":Test_Query_Data_Equal end at "
                + System.currentTimeMillis());

        return list.size();
    }

    public int Test_Query_Data_Like(String str) {
        System.out.println(getClass() + ":Test_Query_Data_Like start at "
                + System.currentTimeMillis());
        ArrayList list = this.sqlExcute.query(Test_Class_SmallData_String.class
                .getName(), str);
        System.out.println(getClass() + ":Test_Query_Data_Like end at "
                + System.currentTimeMillis());

        return list.size();
    }

    public void startTest() {
        Test_create_Data(500);

        int result = Test_Query_Data_Equal("data1 == 'data1_1'");

        if (result == 1)
            System.out.println(getClass()
                    + "Test_Data_Query_Data_Equal1 trace ok");
        else {
            System.out.println(getClass()
                    + "Test_Data_Query_Data_Equal1 trace fail");
        }

        result = Test_Query_Data_Equal("data2 == 'data1_1'");

        if (result == 0)
            System.out.println(getClass() + "Test_Query_Data_Equal2 trace ok");
        else {
            System.out
                    .println(getClass() + "Test_Query_Data_Equal2 trace fail");
        }

        result = Test_Query_Data_Equal("data20 == 'data20_4999'");

        if (result == 1)
            System.out.println(getClass() + "Test_Query_Data_Equal3 trace ok");
        else {
            System.out
                    .println(getClass() + "Test_Query_Data_Equal3 trace fail");
        }

        result = Test_Query_Data_Like("data1 LIKE 'data1_3*'");

        System.out.println(getClass() + "like test1 result is " + result);

        result = Test_Query_Data_Like("data1 LIKE 'data*'");

        System.out.println(getClass() + "like test2 result is " + result);

        result = Test_Query_Data_Equal("data1 != 'data1_3'");

        System.out.println(getClass() + "like test not equal result is "
                + result);
    }
}