package com.localxdata.storage;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import com.localxdata.index.IndexTree;
import com.localxdata.index.IndexUtil;
import com.localxdata.index.Node;
import com.localxdata.sql.SqlUtil;
import com.localxdata.struct.DataCell;
import com.localxdata.util.LogUtil;
import com.localxdata.util.XmlUtil;

public class MemoryData {
    
	public static final String TAG = "MemoryData";
	
    //Original Data!!
    private static HashMap<String, DataCellList> mDataListMap;
    
    public static void createDataList(String name, DataCellList list) {
        mDataListMap.put(name, list);
        SaveHandler.getInstance().addInsertTable(name,0,list.size() - 1);
    }

    public static void removeDataList(String name) {
        mDataListMap.remove(name);
    }

    public static HashMap<String, DataCellList> getDataMap() {
        return mDataListMap;
    }

    public static HashMap<String, DataCellList> createDataMap() {
        return mDataListMap;
    }
    
    public static void init() {
        if(mDataListMap != null) {
            return;
        }
        LogUtil.PRINTMEM(TAG, "initSotrage trace3");
        mDataListMap = new HashMap<String, DataCellList>();
            
        //we should make the files in fileTable in order.!!!
    	//because the file may be out of order.just like:
    	//xxxx_5.xml
    	//xxxx_1.xml
    	//xxxx_0.xml
        ArrayList<String>files = TableControl.getAllFiles();
        
        XmlUtil.getInstance().LoadAllDataXml(mDataListMap,files);
        
    }
    
    public static DataCellList getDataList(String name) {
        return mDataListMap.get(name);
    }
    
    public static void updateData(DataCell datacell,Object srcObj,String[] valueName) {
        
        datacell.setState(DataCell.DATA_UPDATE);
      
        for(String v:valueName) {
            try {
                Field f1 = srcObj.getClass().getField(v);
                Field f2 = datacell.obj.getClass().getField(v);                
                SqlUtil.copyField(f2,f1,datacell.obj,srcObj);
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
        
        SaveHandler.getInstance().addUpdateTable(datacell.obj.getClass().getName(),datacell.getId());
        
        for(String indexName:valueName) {
        	//IndexTree tree = IndexUtil.getInstance().getIndexTree(datacell.obj.getClass().getName(), indexName);
        	
        	Node node = datacell.getNodeMap().get(indexName);
        	if(node!=null) {
        		IndexUtil.getInstance().removeNode(node);
        		IndexUtil.getInstance().insertIndex(datacell,indexName);
        	}
        }
    }
    
    public static DataCell insertData(String className,Object obj) {        
        DataCellList list = mDataListMap.get(className);
        
        if(list == null) {
        	list = new DataCellList();
        	mDataListMap.put(className, list);
        }
        
        DataCell cell = list.insertDataCell(obj);
        cell.setState(DataCell.DATA_INSERT);
        
        IndexUtil.getInstance().insertIndex(cell);
        
        SaveHandler.getInstance().addInsertTable(className,cell.getId());
        
        return cell;
    }
    
    public static DataCell insertDataFromXml(String className,Object obj) {
        DataCellList list = mDataListMap.get(className);
        
        if(list == null) {
        	list = new DataCellList();
        	mDataListMap.put(className, list);
        }
        
        DataCell cell = list.insertDataCell(obj);
        cell.setState(DataCell.DATA_IDLE);
        
        IndexUtil.getInstance().insertIndex(cell);
        
        return cell;
    }
    
    public static void deleteData(String tableName,DataCell data) {
        data.setState(DataCell.DATA_DELETE);
        
        SaveHandler.getInstance().addDeleteTable(tableName,data.getId());
        
        //we should also delete index
        Iterator iterator = data.getNodeMap().keySet().iterator();
        
        while(iterator.hasNext()) {
        	Node node = data.getNodeMap().get(iterator.next());
        	IndexUtil.getInstance().removeNode(node);
        }
        
    }
}