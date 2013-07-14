package com.localxdata.test;

import com.localxdata.util.PraseParamUtil;
import java.io.PrintStream;

public class Test_ParseParamUtil implements TestInf {
    PraseParamUtil mParamUtil = new PraseParamUtil();

    private boolean testPraseObjectName(Object obj) {
        if (PraseParamUtil.PraseObjectName(obj).equals(
                testClass.class.getName())) {
            return true;
        }

        return false;
    }

    public void startTest() {
        System.out.println("Test_ParseParamUtil start test");

        if (!testPraseObjectName(new testClass())) {
            System.out
                    .println("Test_ParseParamUtil testPraseObjectName failed");
            return;
        }
    }
}