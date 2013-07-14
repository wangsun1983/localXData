package com.localxdata.test;

import com.localxdata.util.PraseSqlUtil;
import java.util.ArrayList;

public class Test_ParseSqlUtil implements TestInf {
    PraseSqlUtil mParamUtil = PraseSqlUtil.getInstance();

    private boolean testgetRPN(String sql) {
        ArrayList list = this.mParamUtil.changeSqlToAction(sql);
        return true;
    }

    public void startTest() {
        testgetRPN("!abc && p == 9");
    }
}