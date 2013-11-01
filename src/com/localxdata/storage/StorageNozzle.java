package com.localxdata.storage;

import java.util.HashMap;

import com.localxdata.struct.DataCell;
import com.localxdata.util.LogUtil;

/**
 *This class will provider the interface ~~~
 * 
 * */


public class StorageNozzle {
      
	public static final String TAG = "StorageNozzle";
	
    public static DataCellList getDataList(String className) {
        return MemoryData.getDataList(className);
    }
    
    public static HashMap<String, DataCellList> getAllDataList() {
        return MemoryData.getDataMap();
    }
    
    public static void creatDataList(String className,DataCellList datalist) {
        MemoryData.createDataList(className, datalist);
    }
    
    public static void deleteDataList(String className,DataCellList delList) {
        //MemoryData.
    }
    
    public static void updateData(DataCell datacell,Object srcObj,String[] valueName) {
        MemoryData.updateData(datacell,srcObj,valueName);
    }
    
    public static void updateData(DataCellList list) {
        //TODO
    }
    
    public static DataCell insertData(String className,Object obj) {
        return MemoryData.insertData(className,obj);
    }
    
    public static DataCell insertDataFromXml(String className,Object obj) {
    	return MemoryData.insertDataFromXml(className, obj);
    }
    
    public static void deleteData(String className,DataCell data) {
        MemoryData.deleteData(className, data);
    }

    public static void initStorage() {
    	
    	LogUtil.PRINTMEM(TAG, "initSotrage start");
    	
    	TableControl.init();
    	
    	LogUtil.PRINTMEM(TAG, "initSotrage trace1");
    	
        SaveHandler.getInstance().init();
        LogUtil.PRINTMEM(TAG, "initSotrage trace2");
        
        MemoryData.init();
        LogUtil.PRINTMEM(TAG, "initSotrage trace3");
        
        LogUtil.PRINTMEM(TAG, "initSotrage end");
    }
}
