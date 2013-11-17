package com.localxdata.sql;


import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.localxdata.index.IndexUtil;
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
    
    private static String TAG = "ExcuteSqlBySingleTable";
    
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
    	
    	final ArrayList<Object> list = new ArrayList<Object>();
    	
    	final Queue<Object>queue = new ConcurrentLinkedQueue<Object>(); 
    	//wangsl use parallel
    	ExecutorService exec = Executors.newFixedThreadPool(10);
    	
    	
    	if(dataList != null) {
    	    dataList.startLoopRead();
    	    Iterator<DataCell> iterator = dataList.getIterator();
    	    while(iterator.hasNext()) {
    	    	final DataCell cell = iterator.next();
    	    	if(cell.getState() != DataCell.DATA_DELETE) {
                    //list.add(SqlUtil.copyObj(dataCell.obj));
    	    		exec.execute(new Runnable() {
						@Override
						public void run() {
							queue.add(SqlUtil.copyObj(cell.obj));
						}
    	    			
    	    		});
    	    	}
    	    }
    	    
    	    dataList.finishLoopRead();
    	}
    	//wangsl use parallel
    	
    	exec.shutdown();
    	try {
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return new ArrayList(queue);
    }
    
    public ArrayList<Object> query(String tableName,String sql) {
        
    	if(sql == null || sql.length() == 0) {
    		return query(tableName);
    	}
    	
        ArrayList<Object> list = new ArrayList<Object>();
        
        ArrayList<Action>actionList = mPraseSqlInstance.changeSqlToAction(sql);
                        
        
        DataCellList dataList = StorageNozzle.getDataList(tableName);
        
        if(SqlUtil.canUseIndex(actionList, tableName)) {
        	list = SqlUtil.checkDataByIndex(tableName,actionList,SqlUtil.SEARCH_REASON_QUERY);
        	return (ArrayList<Object>) SqlUtil.copyObjMemory(list);
        }else {
        	ActionTreeNode node = mPraseSqlInstance.changeActionListToTree(actionList);
        	
            dataList.startLoopRead();
            Iterator<DataCell>iterator = dataList.getIterator();
            
            while(iterator.hasNext()) {
            	DataCell datacell = iterator.next();
            	if(datacell.getState() == DataCell.DATA_DELETE) {
                    continue;
                }
              
                if(SqlUtil.checkDataByTree(datacell.obj,node)) {
                    list.add(SqlUtil.copyObj(datacell.obj));
                }
            }
            
            dataList.finishLoopRead();
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
        
        ArrayList<Object> deleteList = new ArrayList<Object>();
        
        //if there is only a sql action,we can user index to do remove action...
        //else if there are a few sql actions,but all the actions can be able to
        //excuted by index,we can do query first,then remove the query result..
        if(SqlUtil.canUseIndex(actionList, tableName)) {
        	LogUtil.d(TAG, "use index");
        	
        	deleteList = SqlUtil.checkDataByIndex(tableName,actionList,SqlUtil.SEARCH_REASON_DEL);
            //    dataList.removeAll(deleteList);
        	for(Object obj :deleteList) {
        		if(obj instanceof DataCell) {
        			DataCell cell = (DataCell)obj;
        		    StorageNozzle.deleteData(tableName, cell);
        		}
        	}
        } else {
            dataList.startLoopRead();
            
            Iterator<DataCell>iterator = dataList.getIterator();
            while(iterator.hasNext()) {
            	DataCell datacell = iterator.next();
            	if(SqlUtil.checkDataByAction(datacell.obj,actionList)) {
                    StorageNozzle.deleteData(tableName, datacell);
                } 
            }
            dataList.finishLoopRead();
        }
    }
    
    //This method also need to use index to del.....
    public void delete(Object obj) {
        
        if(obj == null) {
            return;
        }
        
        //Get dataTable;
        String className = obj.getClass().getName();
        
        DataCellList dataList = StorageNozzle.getDataList(className);
        
        Field[]fields1 = obj.getClass().getDeclaredFields();
        int length = fields1.length;
        
        dataList.startLoopRead();
        Iterator<DataCell>iterator = dataList.getIterator();
        
        while(iterator.hasNext()) {
        	boolean isTheData = true;
        	DataCell datacell = iterator.next();
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
        
        dataList.finishLoopRead();
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
        
        ArrayList<Object>insertList = (ArrayList<Object>)SqlUtil.copyObjMemory(objList);
        
        for(Object obj:insertList) { 
            StorageNozzle.insertData(tableName,obj);
        }
        
        return true;
    }
    
    public boolean update(Object data,String valueName[],String sql) {
        
        String tableName = data.getClass().getName();
        
        DataCellList dataList = StorageNozzle.getDataList(tableName);
        
        ArrayList<Action>actionList = mPraseSqlInstance.changeSqlToAction(sql);
        
        if(dataList == null) {
            return false;
        }

        if(SqlUtil.canUseIndex(actionList, tableName)) {
        	ArrayList<Object> deleteList = new ArrayList<Object>();
        	
        	deleteList = SqlUtil.checkDataByIndex(tableName,actionList,SqlUtil.SEARCH_REASON_DEL);
        	
        	for(Object obj :deleteList) {
        		if(obj instanceof DataCell) {
        			DataCell cell = (DataCell)obj;
        		    StorageNozzle.updateData(cell, data, valueName);
        		}
        	}
        	
        } else {
            dataList.startLoopRead();
            Iterator<DataCell>iterator = dataList.getIterator();
            while(iterator.hasNext()) {
            	DataCell datacell = iterator.next();
            	if(SqlUtil.checkDataByAction(datacell.obj,actionList)) {
                    StorageNozzle.updateData(datacell, data, valueName);
                } 
            }
            
            dataList.finishLoopRead();
        }        
        
        return true;
    }
    
}
