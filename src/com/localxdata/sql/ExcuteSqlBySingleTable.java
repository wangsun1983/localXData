package com.localxdata.sql;


import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.localxdata.index.IndexTree;
import com.localxdata.index.IndexUtil;
import com.localxdata.index.Node;
import com.localxdata.util.LogUtil;
import com.localxdata.util.PraseSqlUtil;
import com.localxdata.util.XmlUtil;
import com.localxdata.util.PraseSqlUtil.Action;
import com.localxdata.util.PraseSqlUtil.ActionTreeNode;


public class ExcuteSqlBySingleTable {
    
    protected PraseSqlUtil mPraseSqlInstance;
    protected XmlUtil mXmlUtil;
    protected static HashMap<String,ArrayList<Object>>mTableHashMap;
    
    protected static ExcuteSqlBySingleTable mInstance = null;
    
    protected StoreSqlThread mStoreSqlThread = null;
    
    public static ExcuteSqlBySingleTable getInstance() {
        
        if(mInstance == null) {
            mInstance = new ExcuteSqlBySingleTable();
        }
        
        return mInstance;
    }
    
    private ExcuteSqlBySingleTable() {
        mPraseSqlInstance = PraseSqlUtil.getInstance();
        mXmlUtil = XmlUtil.getInstance();
        mTableHashMap = mXmlUtil.LoadAllXml();
        mStoreSqlThread = new StoreSqlThread();
        
        //TODO
        //the save thread should be start by main function
        mStoreSqlThread.setStoreDataBase(mTableHashMap);
        mStoreSqlThread.start();
        //TODO
    }
    
    @SuppressWarnings("unchecked")
    public void creatTable(String tableName,ArrayList data) {
        mTableHashMap.put(tableName, data);
        mStoreSqlThread.addModifiedTable(tableName);
    }
    
    //Single Table start
    public ArrayList<Object> query(String tableName) {
    	ArrayList<Object>dataList = mXmlUtil.LoadXml_Sax(tableName);
    	
    	ArrayList<Object> list = new ArrayList<Object>();
    	for(Object obj :dataList) {
            list.add(SqlUtil.copyObj(obj));
        }
    	
    	return list;
    }
    
    public ArrayList<Object> query(String tableName,String sql) {
        
    	if(sql == null || sql.length() == 0) {
    		return query(tableName);
    	}
    	
        ArrayList<Object> list = new ArrayList<Object>();
        //Get Acton List
        ArrayList<Action>actionList = mPraseSqlInstance.changeSqlToAction(sql);
        
        ArrayList<Object>dataList = mTableHashMap.get(tableName);
        
        if(dataList == null) {
            return null;
        }
        
        //wangsl test
        //for(Object obj :dataList) {
        //    if(checkDataByAction(obj,actionList)) {
        //        list.add(copyObj(obj));
        //    }   
        //}
        //wangsl test
        if (actionList.size() == 1) {
            PraseSqlUtil.Action act = (PraseSqlUtil.Action) actionList.get(0);

            if ((act instanceof PraseSqlUtil.ComputeAction)) {
                PraseSqlUtil.ComputeAction computeAct = (PraseSqlUtil.ComputeAction) act;
                if ((computeAct.mAction == 1)
                        && ((computeAct.mDataType == 0) || (computeAct.mDataType == 1))) {
                    IndexTree index = IndexUtil.getInstance().getIndexTree(tableName,
                            computeAct.mFieldName);

                    Node node = IndexUtil.getInstance().searchNode(index,
                            computeAct.mAction,
                            Integer.valueOf(computeAct.mData));

                    ArrayList list1 = new ArrayList();
                    IndexUtil.getInstance().changeIndexToList(list1, node);
                    LogUtil.d("ExcuteSqlBySingleTable", "list size is "
                            + list1.size() + "end at "
                            + System.currentTimeMillis());
                }

            }

        }
        
        //wangsl
        
        ActionTreeNode node = mPraseSqlInstance.changeActionListToTree(actionList);

        //wangsl
        HashSet<Object> result = SqlUtil.checkDataByTree(tableName,node);
        System.out.println("result size is " + result.size());
        
        //wangsl
        
        for(Object obj :dataList) {
            if(SqlUtil.checkDataByTree(obj,node)) {
                list.add(SqlUtil.copyObj(obj));
            }   
        }
        //wangsl
        
        return list;
    }
    //Single Table end
    
    public void delete(String tableName,String sql) {
        if(sql == null || sql.length() == 0) {
            return;
        }
        
        //Get Acton List
        ArrayList<Action>actionList = mPraseSqlInstance.changeSqlToAction(sql);
        
        ArrayList<Object>dataList = mTableHashMap.get(tableName);
        
        for(Object obj :dataList) {
            if(SqlUtil.checkDataByAction(obj,actionList)) {
                dataList.remove(obj);
            }   
        }
        mStoreSqlThread.addModifiedTable(tableName);
    }
    
    public void delete(Object obj) {
        
        if(obj == null) {
            return;
        }
        
        //Get dataTable;
        String tableName = obj.getClass().getName();
        
        ArrayList<Object>dataList = mTableHashMap.get(tableName);
        
        Field[]fields1 = obj.getClass().getDeclaredFields();
        int length = fields1.length;
        ArrayList<Object>removeList = new ArrayList<Object>();
        
        for(Object o :dataList) {
            boolean isTheData = true;
            Field[] fields2 = o.getClass().getDeclaredFields();
            for(int i = 0; i < length;i++) {
                if(SqlUtil.compareField(o,obj,fields2[i],fields1[i],Action.SQL_ACTION_EQUAL)) {
                    continue;
                }else {
                    isTheData = false;
                    break;
                }
            }
            
            if(isTheData) {
                removeList.add(o);
            }
        }
        
        for(Object removeO:removeList) {
            dataList.remove(removeO);
        }
        
        mStoreSqlThread.addModifiedTable(tableName);
    }
    
    public boolean insert(Object obj) {
        if(obj == null) {
            return false;
        }
        
        String tableName = obj.getClass().getName();
        
        ArrayList<Object>dataList = mTableHashMap.get(tableName);
        
        if(dataList == null) {
        	dataList = new ArrayList<Object>();
        	dataList.add(obj);
        	this.creatTable(tableName, dataList);
        	return true;
        }
        
        Object o = SqlUtil.copyObj(obj);
        
        dataList.add(o);
        
        mStoreSqlThread.addModifiedTable(tableName);
        
        return true;
    }
    
    public boolean update(Object data,String valueName[],String sql) {
        
        String tableName = data.getClass().getName();
        
        ArrayList<Object>dataList = mTableHashMap.get(tableName);
        
        if(dataList == null) {
            return false;
        }
        
        ArrayList<Action>actionList = mPraseSqlInstance.changeSqlToAction(sql);
        
        for(Object obj :dataList) {
            if(SqlUtil.checkDataByAction(obj,actionList)) {
                for(String v:valueName) {
                    try {
                        Field f1 = data.getClass().getField(v);
                        Field f2 = obj.getClass().getField(v);
                        
                        SqlUtil.copyField(f2,f1,obj,data);
                    } catch (SecurityException e) {
                        e.printStackTrace();
                    } catch (NoSuchFieldException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    }
                }
            }   
        }
        
        mStoreSqlThread.addModifiedTable(tableName);
        
        return true;
    }
    
}
