package com.localxdata.index;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;

import com.localxdata.struct.DataCell;

public class IndexUtil {
    private static IndexUtil mInstance;
    private HashMap<String, IndexTree> mIndexMap;

    public static IndexUtil getInstance() {
        if (mInstance == null) {
            mInstance = new IndexUtil();
        }

        return mInstance;
    }

    private IndexUtil() {
        this.mIndexMap = new HashMap();
    }

    public IndexTree getIndexTree(String table, String member) {
        String indexStr = table + ">" + member;
        IndexTree index = (IndexTree) this.mIndexMap.get(indexStr);

        if (index == null) {
            index = new IndexTree();
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

    public Node searchNode(IndexTree tree, DataCell datacell, Comparable data) {
        return tree.getNode(datacell, data);
    }

    public Node searchNode(IndexTree tree, int action, Comparable data) {
        return tree.getNode(action, data);
    }

    public void changeIndexToList(ArrayList<Object> list, Node index) {
        list.add(index.dataCell.obj);
        if (index.left != null) {
            changeIndexToList(list, index.left);
        }

        if (index.equal != null) {
            changeIndexToList(list, index.equal);
        }

        if (index.right != null)
            changeIndexToList(list, index.right);
    }
}