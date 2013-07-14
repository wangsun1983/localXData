package com.localxdata.util;

import java.util.ArrayList;
import java.util.HashMap;

public class MemoryDataUtil {
    private static HashMap<String, ArrayList<Object>> mDataListMap;

    public static void addList(String name, ArrayList<Object> list) {
        if (mDataListMap == null) {
            mDataListMap = new HashMap();
        }
        mDataListMap.put(name, list);
    }

    public static void removeList(String name) {
        mDataListMap.remove(name);
    }

    public static HashMap<String, ArrayList<Object>> getDataMap() {
        return mDataListMap;
    }

    public static HashMap<String, ArrayList<Object>> createDataMap() {
        if (mDataListMap == null) {
            mDataListMap = new HashMap();
        }

        return mDataListMap;
    }
}