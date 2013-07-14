package com.localxdata.test;

public class Main {
    public static void main(String[] args) {
        TestInf xmlutil = new Test_XmlUtil();

        Test_XmlUtil testXmlUtil = new Test_XmlUtil();
        testXmlUtil.startTest();

        Test_ExcuteSql_Int excuteSqlInt = new Test_ExcuteSql_Int();
        excuteSqlInt.startTest();
    }
}