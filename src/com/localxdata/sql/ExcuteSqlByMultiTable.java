package com.localxdata.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.localxdata.util.LogUtil;
import com.localxdata.util.PraseSqlUtil;
import com.localxdata.util.XmlUtil;
import com.localxdata.util.PraseSqlUtil.Action;
import com.localxdata.util.PraseSqlUtil.ActionTreeNode;
import com.localxdata.util.PraseSqlUtil.ComputeAction;

public class ExcuteSqlByMultiTable {
    
    protected PraseSqlUtil mPraseSqlInstance;
    protected XmlUtil mXmlUtil;
    protected static HashMap<String,ArrayList<Object>>mTableHashMap;
    
    protected static ExcuteSqlByMultiTable mInstance = null;
    
    private static final String DEBUG_TAG = "ExcuteSqlByMultiTable";
    
    private ExcuteSqlByMultiTable() {
        mPraseSqlInstance = PraseSqlUtil.getInstance();
        mXmlUtil = XmlUtil.getInstance();
        mTableHashMap = mXmlUtil.LoadAllXml();
    }
    
    public static ExcuteSqlByMultiTable getInstance() {
        if(mInstance == null) {
            mInstance = new ExcuteSqlByMultiTable();
        }
        return mInstance;
    }
    
    /**
     * return ArrayList<ArrayList>;
     * For example
     * 1.tableName[] = {"student","family"},return value is
     * {{student.class,family.class},{student.class,family.class}...}
     * 
     * 2.joinArgs = "com.student.id = com.subject._id"
     * 
     * 3.sql = "com.student.id == 5"
     * 
     * 
     **/  
    public ArrayList<ArrayList<Object>> query(String tableName[],String joinArgs,String sql) {
        
    	//We should check the table and join condition
    	ArrayList<Action>joinAction = mPraseSqlInstance.changeSqlToAction(joinArgs);
    	
    	if(!checkJoinArg(tableName,joinAction)) {
    		throw new IllegalArgumentException();
    	}
    	
    	ActionTreeNode joinNode = mPraseSqlInstance.changeActionListToTree(joinAction);
    	
    	ArrayList<ArrayList<Object>> resultList = new ArrayList<ArrayList<Object>>();
        ArrayList<ArrayList<Object>>dataList = new ArrayList<ArrayList<Object>>();
        ActionTreeNode actionNode = null;
        
        if(sql != null) {
        	ArrayList<Action>sqlAction = mPraseSqlInstance.changeSqlToAction(sql);
        	actionNode = mPraseSqlInstance.changeActionListToTree(sqlAction);
        }
        
        HashMap <String,Integer>cursorMap = new HashMap<String,Integer>();
        
        for(String table:tableName) {
        	ArrayList<Object> data = mXmlUtil.LoadXml_Sax(table);
            cursorMap.put(table, data.size() - 1);
            dataList.add(data);
        }
        
        LogUtil.d(DEBUG_TAG,"query start at:" + System.currentTimeMillis());
        
        //start deal
        int cursor = 0;
        
        while(true) {      	
        	if(cursorMap.get(tableName[tableName.length - 1]) == -1 && cursor == -1) {
        		break;
        	}
        	
        	int count = 0;
        	
        	HashMap<String,Object>checkdata = new HashMap<String,Object>();

        	for(ArrayList<Object> objlist:dataList) {
        		Object obj = objlist.get(0);
        		cursor = cursorMap.get(obj.getClass().getName());
        		
        		
        		if(cursor >= 0) {        			
        		    checkdata.put(obj.getClass().getName(), objlist.get(cursor));
        		    if(count == 0) {
        		        cursor--;
        		        cursorMap.put(obj.getClass().getName(),cursor);
        		    }
        		}
        		
        		//get a list whole data from all the select table.
        		else if(cursor < 0) {
        			//LogUtil.d(DEBUG_TAG, "count trace ");
        			
        			    
        			for(int i = count + 1;i < dataList.size();i++) {
        				Object nextObj = dataList.get(i).get(0);
            		    int nextObjCursor = cursorMap.get(nextObj.getClass().getName());
            		    nextObjCursor--;
            		    boolean isNeedContinue = false;
            		    if(nextObjCursor < 0) {
            		    	if(i != dataList.size() - 1) {
            		    	    nextObjCursor = dataList.get(i).size() - 1;
            		    	    isNeedContinue = true;
            		    	}
            		    } else {
            		    	cursor = objlist.size() - 1;
            		    	checkdata.put(obj.getClass().getName(), objlist.get(cursor));
                			cursorMap.put(obj.getClass().getName(),cursor);
                			
            		    }
            		    
            		    cursorMap.put(nextObj.getClass().getName(),nextObjCursor);
            		    if(!isNeedContinue) {
            		    	break;
            		    }
        		    }
        		}
        		//do the select,whether the data fulfill with the args 
            	//we do join judgement first
            	count++;
        	}
        	
        	if(checkdata.size() != 0) {
        	    if(!SqlUtil.checkMultiJointByTree(checkdata, joinNode)) {
        		    continue;
        	    }
        	
        	    if(actionNode != null) {
        		    if(!SqlUtil.checkMultiDataByTree(checkdata, actionNode)) {
        			    continue;
        		    }
        	    }
            
        	    //if both is ok,we should add to result arraylist
        	    ArrayList<Object>resultRaw = new ArrayList<Object>();
        	    for(String table:tableName) {
        		    resultRaw.add(checkdata.get(table));
        	    }
        	    resultList.add(resultRaw);
        	}
        }
       
        LogUtil.d(DEBUG_TAG,"query start end:" + System.currentTimeMillis());
        
        return resultList;
    }
    
    public void Delete(String tableName[],String joinArgs,String sql) {
    	//TODO
    }
    
    
    private boolean checkJoinArg(String tableName[],ArrayList<Action>joinAction) {
    	
    	HashSet<String>tableSet = new HashSet<String>();

    	int joinActionSize = joinAction.size();
    	
    	for(int i = 0;i < joinActionSize;i++) {
    		ComputeAction act = (ComputeAction) joinAction.get(0);
    		String table1[] = SqlUtil.changeMultiFieldToTableAndId(act.mData);
    		String table2[] = SqlUtil.changeMultiFieldToTableAndId(act.mFieldName);
    		tableSet.add(table1[SqlUtil.MULTI_CLASS_FIELD]);
    		tableSet.add(table2[SqlUtil.MULTI_CLASS_FIELD]);
    	}
    	
    	if(tableSet.size() != tableName.length) {
    	    return false;
    	}
    	
    	return true;
    }
    
}
