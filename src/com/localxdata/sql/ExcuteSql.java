package com.localxdata.sql;

import com.localxdata.util.XmlUtil;
import java.util.ArrayList;

public class ExcuteSql {
    private XmlUtil mXmlUtil;
    private ExcuteSqlByMultiTable mExcuteSqlByMultiTable;
    private ExcuteSqlBySingleTable mExcuteSqlBySingle;
    private static ExcuteSql instance;

    private ExcuteSql() {
        this.mXmlUtil = XmlUtil.getInstance();
        this.mExcuteSqlByMultiTable = ExcuteSqlByMultiTable.getInstance();
        this.mExcuteSqlBySingle = ExcuteSqlBySingleTable.getInstance();
    }

    public static ExcuteSql getInstance() {
        if (instance == null) {
            instance = new ExcuteSql();
        }

        return instance;
    }

    public void setDbPath(String filePath) {
        this.mXmlUtil.setXmlRootDir(filePath);
    }

    public ArrayList<ArrayList<Object>> query(String[] tableName,
            String joinArgs, String sql) {
        return this.mExcuteSqlByMultiTable.query(tableName, joinArgs, sql);
    }

    public ArrayList<Object> query(String tableName, String sql) {
        return this.mExcuteSqlBySingle.query(tableName, sql);
    }

    public void delete(String tableName, String sql) {
        this.mExcuteSqlBySingle.delete(tableName, sql);
    }

    public void delete(Object obj) {
        this.mExcuteSqlBySingle.delete(obj);
    }

    public boolean insert(Object obj) {
        return this.mExcuteSqlBySingle.insert(obj);
    }

    public boolean update(Object data, String[] valueName, String sql) {
        return this.mExcuteSqlBySingle.update(data, valueName, sql);
    }
}