package com.localxdata.index;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.localxdata.storage.DataCellList;
import com.localxdata.struct.DataCell;

public class IndexUtil {
    private static IndexUtil mInstance;
    private HashMap<String, IndexTree> mIndexMap;
    
    public static final int SEARCH_REASON_QUERY = 0;
    public static final int SEARCH_REASON_DEL = 1;

    public static IndexUtil getInstance() {
        if (mInstance == null) {
            mInstance = new IndexUtil();
        }

        return mInstance;
    }

    private IndexUtil() {
        this.mIndexMap = new HashMap();
    }

    public boolean isIndexExist(String table,String member) {
    	
        String indexStr = table + ">" + member;
        IndexTree index = (IndexTree) this.mIndexMap.get(indexStr);
        if(index == null) {
        	return false;
        }	
        
        return true;
    }
    
    public IndexTree getIndexTree(String table, String member) {
        String indexStr = table + ">" + member;
        IndexTree index = (IndexTree) this.mIndexMap.get(indexStr);

        if (index == null) {
            index = new IndexTree(table);
            this.mIndexMap.put(indexStr, index);
        }

        return index;
    }

    public void putIndexTree(String table, String member, IndexTree index) {
        String indexStr = table + ">" + member;
        this.mIndexMap.put(indexStr, index);
    }

    public void updateIndex(DataCell datacell) {
        insertIndex(datacell);
    }

    public void insertIndex(DataCell datacell) {
        Field[] fields = datacell.obj.getClass().getFields();
        String table = datacell.obj.getClass().getName();

        for(Field field:fields) {
            String fieldName = field.getName();
            IndexTree tree = getIndexTree(table, fieldName);
            
            try {
                Comparable ele = (Comparable)field.get(datacell.obj);
                tree.add(datacell,ele);
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    public HashSet<Object> searchNode(IndexTree tree, int action, Comparable data,int reason) {
    	return tree.getNode(action, data,reason);
    }
    
    public void removeNode(Node n) {
        n.markDelete();
    }
    
    public void changeIndexToList(HashSet<Object> list, Node index) {
        list.add(index.dataCell.obj);
        if (index.left != null) {
            changeIndexToList(list, index.left);
        }

        if (index.equalList != null) {
            //changeIndexToList(list, index.equal);
        	if(index.equalList.size() != 0) {
        		for(Object obj:index.equalList) {
        			Node n = (Node)obj;
        			list.add(n.dataCell.obj);
        		}
        	}
        }

        if (index.right != null) {
            changeIndexToList(list, index.right);
        }
    }
    
    
    //public void removeByIndex(IndexTree tree, int action, Comparable data) {
    //	tree.removeByIndex(action, data);
    //}
    
    public int predictNodeNum(Node index) {
        int depth = getDepthOfNode(index);
        return (int)(Math.pow((double)2, (double)depth) - 1);
    }
    
    
    private int getDepthOfNode(Node index) {
    	int depth = 0;
    	Node indexLoop = index;
    	
    	while(true) {
    		depth++;
    		if(indexLoop.left != null) {
    		    indexLoop = index.left;
    		    continue;
    		}
    		
    		if(indexLoop.right != null) {
    		    indexLoop = index.right;
    		    continue;
    		}
    		
    		break;
    	}
    	
    	return depth;
    }
    
    
}