package com.localxdata.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.localxdata.config.ConfigNozzle;
import com.localxdata.struct.DataTableControl;
import com.localxdata.util.XmlUtil;

/**
 * We should make a table to save all the relation
 * between db and class
 * */
public class TableControl {
    public static String TABLE_CONTROL_FILE = "table4db.xml";
    public static String TABLE_TAG = " ";
    
    public static HashMap<String,DataTableControl> mTableMap; 
    
    //TODO
    public static String addTable(String tableName,int start,int end) {
        
        int searchId = 0;
        String searchTableName = null;
        for(;searchId <Integer.MAX_VALUE;searchId++) {
            searchTableName = tableName + TABLE_TAG + searchId;
            
            if(mTableMap.get(searchTableName) == null) {
                break;
            }    
        }
        
        DataTableControl data = new DataTableControl(start,end);
        mTableMap.put(searchTableName, data);
        
        try {
            String fileName = ConfigNozzle.CONFIG_LOCAL_DATA_ENGINE_ROOT 
                              + TABLE_CONTROL_FILE;
            
            XmlUtil.getInstance().CreateDataTableXml(fileName,mTableMap);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return searchTableName;
    }
    
    /**
     * this function is only provider for create table which has not been created before.
     * */
    public static void addExactTable(String className,int start,int end,int blockname) {
    	String filename = className + "_" + blockname;
    	DataTableControl data = new DataTableControl(start,end);
        mTableMap.put(filename, data);
        try {
            String fileName = ConfigNozzle.CONFIG_LOCAL_DATA_ENGINE_ROOT 
                              + TABLE_CONTROL_FILE;
            
            XmlUtil.getInstance().CreateDataTableXml(fileName,mTableMap);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
    
    public static String findTable(String tableName,int pos) {
        
        int searchId = 0;
        String searchTableName = null;
        for(;searchId <Integer.MAX_VALUE;searchId++) {
            searchTableName = tableName + TABLE_TAG + searchId;
            
            DataTableControl data = mTableMap.get(searchTableName);
            
            if(data != null) {
                if(pos <= data.end && pos >= data.start) {
                    break;
                }
            }    
        }
        
        return searchTableName;
    }
    
    public static ArrayList<String> findTables(String className) {
        int searchId = 0;
        String searchTableName = null;
        ArrayList<String> tables = new ArrayList<String>();
        
        for(;searchId <Integer.MAX_VALUE;searchId++) {
            searchTableName = className + TABLE_TAG + searchId;
            DataTableControl data = mTableMap.get(searchTableName);
            if(data != null) {
                tables.add(searchTableName);
            }else {
            	break;
            }
        }
        
        return tables;
    }
    
    public static boolean isTableExists(String fileName) {
    	if(mTableMap.get(fileName) != null) {
    		return true;
    	}
    	
    	return false;
    }
    
    public static void init() {
        String fileName = ConfigNozzle.CONFIG_LOCAL_DATA_ENGINE_ROOT 
                            + TABLE_CONTROL_FILE;
        File file = new File(fileName);
        
        mTableMap = new HashMap<String,DataTableControl>();
        
        if(file.exists()) {
            XmlUtil.getInstance().LoadDataTableXml(fileName,mTableMap);
        }
    }
    
    public static ArrayList<String>getAllFiles() {
    	HashSet<String>classList = getClassTable();
    	ArrayList<String>files = new ArrayList<String>();
    	
    	for(String classname:classList) {
    	    for(int i = 0;i<Integer.MAX_VALUE;i++) {
    	    	String fileName = XmlUtil.getInstance().transformFullPath(classname, i);
    	    	
    	    	File f = new File(fileName);
    	    	if(f.exists()) {
    	    		files.add(XmlUtil.getInstance().transformFileName(classname, i));
    	    	}else {
    	    		break;
    	    	}
    	    }
    	}
    	
    	return files;
    }
    
    private static HashSet<String> getClassTable() {
    	HashSet<String>result = new HashSet<String>();
    	
        Iterator iter = mTableMap.entrySet().iterator();
        
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Object key = entry.getKey();
            String fileName = String.valueOf(key);
            String className = XmlUtil.getInstance().transformClassName(fileName);//fileName.substring(0,fileName.lastIndexOf("_"));
            result.add(className);
        }
    	
    	return result;
    }
}
