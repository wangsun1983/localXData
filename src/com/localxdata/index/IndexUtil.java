package com.localxdata.index;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;

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
            index = new IndexTree(member);
            this.mIndexMap.put(indexStr, index);
        }

        return index;
    }

    public void putIndexTree(String table, String member, IndexTree index) {
        String indexStr = table + ">" + member;
        this.mIndexMap.put(indexStr, index);
    }

    public void updateIndex(Object obj) {
        insertIndex(obj);
    }

    public void insertIndex(Object obj) {
        Field[] fields = obj.getClass().getFields();
        String table = obj.getClass().getName();

        for(Field field:fields) {
            String fieldName = field.getName();
            IndexTree tree = getIndexTree(table, fieldName);
            
            try {
                Comparable ele = (Comparable)field.get(obj);
                tree.add(obj,ele);
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    public Node searchNode(IndexTree tree, Object obj, Comparable data) {
        return tree.getNode(obj, data);
    }

    public Node searchNode(IndexTree tree, int action, Comparable data) {
        return tree.getNode(action, data);
    }

    public void changeIndexToList(ArrayList<Object> list, Node index) {
        list.add(index.obj);
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