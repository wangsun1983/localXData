package com.localxdata.sql;


import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.localxdata.index.IndexTree;
import com.localxdata.index.IndexUtil;
import com.localxdata.index.Node;
import com.localxdata.storage.DataCellList;
import com.localxdata.storage.StorageNozzle;
import com.localxdata.struct.DataCell;
import com.localxdata.util.LogUtil;
import com.localxdata.util.PraseSqlUtil;
import com.localxdata.util.XmlUtil;
import com.localxdata.util.PraseSqlUtil.Action;
import com.localxdata.util.PraseSqlUtil.ActionTreeNode;


public class ExcuteSqlBySingleTable {
    
    protected PraseSqlUtil mPraseSqlInstance;
    protected XmlUtil mXmlUtil;
    //protected static HashMap<String,DataCellList>mTableHashMap;
    
    protected static ExcuteSqlBySingleTable mInstance = null;
        
    public static ExcuteSqlBySingleTable getInstance() {
        
        if(mInstance == null) {
            mInstance = new ExcuteSqlBySingleTable();
        }
        
        return mInstance;
    }
    
    private ExcuteSqlBySingleTable() {
        mPraseSqlInstance = PraseSqlUtil.getInstance();
        mXmlUtil = XmlUtil.getInstance();
        //mTableHashMap = StorageNozzle.getAllDataList();
    }
    
    //Single Table start
    public ArrayList<Object> query(String tableName) {
    	//ArrayList<Object>dataList = mXmlUtil.LoadXml_Sax(tableName);
        DataCellList dataList = StorageNozzle.getDataList(tableName);
    	
    	ArrayList<Object> list = new ArrayList<Object>();
    	
    	if(dataList != null) {
    	    dataList.enterLooper();
    	    for(DataCell dataCell :dataList) {
    	    	if(dataCell.getState() != DataCell.DATA_DELETE) {
                    list.add(SqlUtil.copyObj(dataCell.obj));
    	    	}
            }
    	    dataList.leaveLooper();
    	}

    	return list;
    }
    
    public ArrayList<Object> query(String tableName,String sql) {
        
    	if(sql == null || sql.length() == 0) {
    		return query(tableName);
    	}
    	
        ArrayList<Object> list = new ArrayList<Object>();
        
        ArrayList<Action>actionList = mPraseSqlInstance.changeSqlToAction(sql);
                        
        
        DataCellList dataList = StorageNozzle.getDataList(tableName);
        
        if(SqlUtil.canUseIndex(actionList, tableName)) {
        	list = SqlUtil.checkDataByIndex(dataList, tableName,actionList);
        }else {
        	ActionTreeNode node = mPraseSqlInstance.changeActionListToTree(actionList);
        	
            dataList.enterLooper();
            for(DataCell datacell :dataList) {
                if(datacell.getState() == DataCell.DATA_DELETE) {
                    continue;
                }
              
                if(SqlUtil.checkDataByTree(datacell.obj,node)) {
                    list.add(SqlUtil.copyObj(datacell.obj));
                }   
            }
            dataList.leaveLooper();
        }
        
        return list;
    }
    
    
    //Single Table end
    public void delete(String tableName,String sql) {
        if(sql == null || sql.length() == 0) {
            return;
        }
        
        //Get Acton List
        ArrayList<Action>actionList = mPraseSqlInstance.changeSqlToAction(sql);
        
        DataCellList dataList = StorageNozzle.getDataList(tableName);
        
        DataCellList deleteList = new DataCellList(); 
        
        dataList.enterLooper();
        for(DataCell datacell :dataList) {
            if(SqlUtil.checkDataByAction(datacell.obj,actionList)) {
                StorageNozzle.deleteData(tableName, datacell);
            }   
        }
        dataList.leaveLooper();
    }
    
    public void delete(Object obj) {
        
        if(obj == null) {
            return;
        }
        
        //Get dataTable;
        String className = obj.getClass().getName();
        
        DataCellList dataList = StorageNozzle.getDataList(className);
        
        Field[]fields1 = obj.getClass().getDeclaredFields();
        int length = fields1.length;
        
        dataList.enterLooper();
        for(DataCell datacell :dataList) {
            boolean isTheData = true;
            Field[] fields2 = datacell.obj.getClass().getDeclaredFields();
            for(int i = 0; i < length;i++) {
                if(SqlUtil.compareField(datacell.obj,obj,fields2[i],fields1[i],Action.SQL_ACTION_EQUAL)) {
                    continue;
                }else {
                    isTheData = false;
                    break;
                }
            }
            
            if(isTheData) {
                StorageNozzle.deleteData(className, datacell);
            }
        }
        dataList.leaveLooper();
    }
    
    public boolean insert(Object obj) {
        if(obj == null) {
            return false;
        }
        
        String className = obj.getClass().getName();
        
        Object o = SqlUtil.copyObj(obj);
        StorageNozzle.insertData(className,o);
        
        return true;
    }
    
    public boolean insert(ArrayList<Object> objList) {
        if(objList == null) {
            return false;
        }
        
        String tableName = objList.get(0).getClass().getName();
        
        for(Object obj:objList) {
            Object o = SqlUtil.copyObj(obj);    
            StorageNozzle.insertData(tableName, o);
        }
        
        return true;
    }
    
    public boolean update(Object data,String valueName[],String sql) {
        
        String tableName = data.getClass().getName();
        
        DataCellList dataList = StorageNozzle.getDataList(tableName);
        
        if(dataList == null) {
            return false;
        }
        
        ArrayList<Action>actionList = mPraseSqlInstance.changeSqlToAction(sql);
        
        dataList.enterLooper();
        for(DataCell datacell :dataList) {
            if(SqlUtil.checkDataByAction(datacell.obj,actionList)) {
                StorageNozzle.updateData(datacell, data, valueName);
            }   
        }
        dataList.leaveLooper();
        
        return true;
    }
    
}
