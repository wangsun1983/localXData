package com.localxdata.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.localxdata.util.LogUtil;
import com.localxdata.util.XmlUtil;

public class StoreSqlThread extends Thread{

    public static int INTERVAL_SHORT = 1000;
    public static int INTERVAL_MIDIUM = 1000 * 60;
    public static int INTERVAL_LONG = 1000*60*15;
    
    private static final String DEBUG_TAG = "StoreSqlThread";
    
    private int mInterval = INTERVAL_SHORT;
    
    private XmlUtil mXmlUtilInstance;
    
    private static HashMap<String,ArrayList<Object>> mStoreMap;
    
    private HashSet<String>modifiedTable = new HashSet<String>();
    
    private boolean isNeedRun = true;
    
    public StoreSqlThread() {
        mXmlUtilInstance = XmlUtil.getInstance();
    }
    
    public void setStoreDataBase(HashMap<String,ArrayList<Object>> map) {
        mStoreMap = map;
    }
    
    public void addModifiedTable(String table) {
        synchronized(modifiedTable) {
            modifiedTable.add(table);
        }
    }
    
    public void setInverval(int interval) {
        mInterval = interval;
    }
    
    public void stopThread() {
        isNeedRun = false;    
    }
    
    public void run() {
        while(isNeedRun) {
            if(modifiedTable.size() == 0) {
                try {
                    //LogUtil.d(DEBUG_TAG,"sleep!!!");
                    Thread.sleep(mInterval);
                    continue;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            
            Object modifiedArray[] = null;
            int size = 0;
            
            synchronized(modifiedTable) {
                modifiedArray = modifiedTable.toArray();
                size = modifiedArray.length;
                modifiedTable.clear();
            }
            
            for(int i=0;i < size;i++) {
            	String table = null;
                
                table = (String)modifiedArray[i];
                LogUtil.d(DEBUG_TAG,"create table is " + table);
                LogUtil.d(DEBUG_TAG,"i is " + i);
                mXmlUtilInstance.CreateXml(new ArrayList<Object>(mStoreMap.get(table)));
            }
            
            //for(String table :modifiedTable) {
            //    LogUtil.d(DEBUG_TAG,"create table is " + table);
            //    synchronized(modifiedTable) {
            //        modifiedTable.remove(table);
            //    }
            //    mXmlUtilInstance.CreateXml(new ArrayList<Object>(mStoreMap.get(table)));
            //}
        }
    }
    
}
