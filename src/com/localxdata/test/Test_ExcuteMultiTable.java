package com.localxdata.test;

import com.localxdata.sql.ExcuteSqlByMultiTable;
import java.io.PrintStream;
import java.util.ArrayList;

public class Test_ExcuteMultiTable implements TestInf {
    public void Test_query() {
        String[] table = { Test_Class_Student.class.getName(),
                Test_Class_Subject.class.getName() };

        String jointArgs = Test_Class_Student.class.getName() + ".age == "
                + Test_Class_Subject.class.getName() + ".age";

        String selectArgs = Test_Class_Student.class.getName() + ".age >= 5 "
                + " && " + Test_Class_Student.class.getName() + ".name != "
                + Test_Class_Subject.class.getName() + ".name";

        ArrayList result = ExcuteSqlByMultiTable.getInstance().query(table,
                jointArgs, selectArgs);
        System.out.println(result.size());
    }

    public void startTest() {
        Test_query();
    }
}